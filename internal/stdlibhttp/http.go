// Package web provides the HTTP surface used by CLIProxyAPI handlers.
// It is implemented only with the Go standard library.
package web

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	DebugMode   = "debug"
	ReleaseMode = "release"
	TestMode    = "test"
	abortIndex  = int8(63)
)

var (
	DefaultWriter      io.Writer = os.Stdout
	DefaultErrorWriter io.Writer = os.Stderr
	DebugPrintFunc               = func(string, ...interface{}) {}
)

type H map[string]any
type HandlerFunc func(*Context)

type Param struct {
	Key   string
	Value string
}

type Params []Param

func (p Params) ByName(name string) string {
	for _, item := range p {
		if item.Key == name {
			return item.Value
		}
	}
	return ""
}

type Error struct {
	Err  error
	Type uint32
	Meta any
}

func (e *Error) Error() string {
	if e == nil || e.Err == nil {
		return ""
	}
	return e.Err.Error()
}

type ErrorList []*Error

func (e ErrorList) ByType(kind uint32) ErrorList {
	out := make(ErrorList, 0, len(e))
	for _, item := range e {
		if item != nil && item.Type&kind != 0 {
			out = append(out, item)
		}
	}
	return out
}

func (e ErrorList) String() string {
	parts := make([]string, 0, len(e))
	for _, item := range e {
		if value := item.Error(); value != "" {
			parts = append(parts, value)
		}
	}
	return strings.Join(parts, "\n")
}

const ErrorTypePrivate uint32 = 1 << 0

type ResponseWriter interface {
	http.ResponseWriter
	http.Flusher
	http.Hijacker
	http.Pusher
	WriteHeaderNow()
	Written() bool
	Status() int
	Size() int
	WriteString(string) (int, error)
}

type responseWriter struct {
	http.ResponseWriter
	status  int
	written bool
	size    int
}

func (w *responseWriter) WriteHeader(status int) {
	if w.written {
		return
	}
	w.status = status
	w.written = true
	w.ResponseWriter.WriteHeader(status)
}

func (w *responseWriter) WriteHeaderNow() { w.WriteHeader(http.StatusOK) }

func (w *responseWriter) Write(data []byte) (int, error) {
	if !w.written {
		w.WriteHeader(http.StatusOK)
	}
	n, err := w.ResponseWriter.Write(data)
	w.size += n
	return n, err
}

func (w *responseWriter) WriteString(data string) (int, error) { return w.Write([]byte(data)) }

func (w *responseWriter) Flush() {
	if !w.written {
		w.WriteHeader(http.StatusOK)
	}
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (w *responseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if hijacker, ok := w.ResponseWriter.(http.Hijacker); ok {
		return hijacker.Hijack()
	}
	return nil, nil, errors.New("http hijacking is not available")
}

func (w *responseWriter) Push(target string, opts *http.PushOptions) error {
	if pusher, ok := w.ResponseWriter.(http.Pusher); ok {
		return pusher.Push(target, opts)
	}
	return http.ErrNotSupported
}

func (w *responseWriter) Status() int {
	if w.status == 0 {
		return http.StatusOK
	}
	return w.status
}
func (w *responseWriter) Size() int     { return w.size }
func (w *responseWriter) Written() bool { return w.written }

type Context struct {
	Request *http.Request
	Writer  ResponseWriter
	Params  Params
	Errors  ErrorList

	engine   *Engine
	keys     map[string]any
	handlers []HandlerFunc
	index    int8
	fullPath string
}

func (c *Context) Deadline() (time.Time, bool) {
	return c.requestContext().Deadline()
}

func (c *Context) Done() <-chan struct{} { return c.requestContext().Done() }
func (c *Context) Err() error            { return c.requestContext().Err() }

func (c *Context) Value(key any) any {
	if value, ok := c.Get(fmt.Sprint(key)); ok {
		return value
	}
	return c.requestContext().Value(key)
}

func (c *Context) requestContext() context.Context {
	if c != nil && c.Request != nil && c.Request.Context() != nil {
		return c.Request.Context()
	}
	return context.Background()
}

func (c *Context) Next() {
	c.index++
	for c.index < int8(len(c.handlers)) {
		c.handlers[c.index](c)
		c.index++
	}
}

func (c *Context) Abort()          { c.index = abortIndex }
func (c *Context) IsAborted() bool { return c.index >= abortIndex }

func (c *Context) Set(key string, value any) {
	if c.keys == nil {
		c.keys = make(map[string]any)
	}
	c.keys[key] = value
}

func (c *Context) Get(key string) (any, bool) {
	value, ok := c.keys[key]
	return value, ok
}

func (c *Context) Error(err error) *Error {
	item := &Error{Err: err, Type: ErrorTypePrivate}
	c.Errors = append(c.Errors, item)
	return item
}

func (c *Context) Header(key, value string) {
	if c.Writer != nil {
		c.Writer.Header().Set(key, value)
	}
}

func (c *Context) GetHeader(key string) string {
	if c.Request == nil {
		return ""
	}
	return c.Request.Header.Get(key)
}

func (c *Context) Query(key string) string {
	if c.Request == nil || c.Request.URL == nil {
		return ""
	}
	return c.Request.URL.Query().Get(key)
}

func (c *Context) GetQuery(key string) (string, bool) {
	if c.Request == nil || c.Request.URL == nil {
		return "", false
	}
	values, ok := c.Request.URL.Query()[key]
	return firstValue(values), ok
}

func (c *Context) QueryArray(key string) []string {
	if c.Request == nil || c.Request.URL == nil {
		return nil
	}
	return append([]string(nil), c.Request.URL.Query()[key]...)
}

func (c *Context) PostForm(key string) string {
	if c.Request == nil {
		return ""
	}
	_ = c.Request.ParseForm()
	return c.Request.PostFormValue(key)
}

func (c *Context) GetRawData() ([]byte, error) {
	if c.Request == nil || c.Request.Body == nil {
		return nil, nil
	}
	data, err := io.ReadAll(c.Request.Body)
	c.Request.Body = io.NopCloser(bytes.NewReader(data))
	return data, err
}

func (c *Context) ShouldBindJSON(target any) error {
	if c.Request == nil || c.Request.Body == nil {
		return io.EOF
	}
	return json.NewDecoder(c.Request.Body).Decode(target)
}

func (c *Context) ShouldBindUri(target any) error {
	if target == nil {
		return errors.New("bind target is nil")
	}
	value := reflect.ValueOf(target)
	if value.Kind() != reflect.Pointer || value.IsNil() || value.Elem().Kind() != reflect.Struct {
		return errors.New("bind target must be a non-nil pointer to a struct")
	}
	value = value.Elem()
	typeOf := value.Type()
	for i := 0; i < value.NumField(); i++ {
		field := value.Field(i)
		if !field.CanSet() {
			continue
		}
		name := typeOf.Field(i).Tag.Get("uri")
		if name == "" {
			continue
		}
		if err := setTextValue(field, c.Param(name)); err != nil {
			return err
		}
	}
	return nil
}

func (c *Context) MultipartForm() (*multipart.Form, error) {
	if c.Request == nil {
		return nil, errors.New("request is nil")
	}
	if err := c.Request.ParseMultipartForm(32 << 20); err != nil {
		return nil, err
	}
	return c.Request.MultipartForm, nil
}

func (c *Context) FormFile(name string) (*multipart.FileHeader, error) {
	if c.Request == nil {
		return nil, errors.New("request is nil")
	}
	_, header, err := c.Request.FormFile(name)
	return header, err
}

func (c *Context) File(path string) {
	if c.Request != nil {
		http.ServeFile(c.Writer, c.Request, path)
	}
}

func (c *Context) FileAttachment(path, name string) {
	c.Writer.Header().Set("Content-Disposition", `attachment; filename="`+name+`"`)
	c.File(path)
}

func (c *Context) Data(status int, contentType string, data []byte) {
	c.Writer.Header().Set("Content-Type", contentType)
	c.Status(status)
	_, _ = c.Writer.Write(data)
}

func (c *Context) JSON(status int, value any) {
	c.Writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	c.Status(status)
	_ = json.NewEncoder(c.Writer).Encode(value)
}

func (c *Context) String(status int, format any, values ...any) {
	c.Writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
	c.Status(status)
	formatString, ok := format.(string)
	if !ok {
		_, _ = fmt.Fprint(c.Writer, format)
		return
	}
	_, _ = fmt.Fprintf(c.Writer, formatString, values...)
}

func (c *Context) Status(status int)        { c.Writer.WriteHeader(status) }
func (c *Context) Param(name string) string { return c.Params.ByName(name) }
func (c *Context) FullPath() string         { return c.fullPath }
func (c *Context) ContentType() string      { return strings.Split(c.GetHeader("Content-Type"), ";")[0] }
func (c *Context) ClientIP() string {
	if forwarded := c.GetHeader("X-Forwarded-For"); forwarded != "" {
		return strings.TrimSpace(strings.Split(forwarded, ",")[0])
	}
	if c.Request == nil {
		return ""
	}
	host, _, err := net.SplitHostPort(c.Request.RemoteAddr)
	if err == nil {
		return host
	}
	return c.Request.RemoteAddr
}

func (c *Context) AbortWithStatus(status int) {
	c.Status(status)
	c.Abort()
}

func (c *Context) AbortWithStatusJSON(status int, value any) {
	c.JSON(status, value)
	c.Abort()
}

type route struct {
	method   string
	pattern  string
	handlers []HandlerFunc
}

type RouteInfo struct {
	Method  string
	Path    string
	Handler string
}

type RouterGroup struct {
	engine   *Engine
	basePath string
	handlers []HandlerFunc
}

func (g *RouterGroup) Group(relativePath string, handlers ...HandlerFunc) *RouterGroup {
	combined := append([]HandlerFunc(nil), g.handlers...)
	combined = append(combined, handlers...)
	return &RouterGroup{engine: g.engine, basePath: joinPath(g.basePath, relativePath), handlers: combined}
}

func (g *RouterGroup) Use(handlers ...HandlerFunc) IRoutes {
	g.handlers = append(g.handlers, handlers...)
	return g
}

func (g *RouterGroup) Handle(method, relativePath string, handlers ...HandlerFunc) IRoutes {
	if g.engine == nil {
		return g
	}
	all := append([]HandlerFunc(nil), g.handlers...)
	all = append(all, handlers...)
	g.engine.routesMu.Lock()
	g.engine.routes = append(g.engine.routes, route{method: strings.ToUpper(method), pattern: joinPath(g.basePath, relativePath), handlers: all})
	g.engine.routesMu.Unlock()
	return g
}

func (g *RouterGroup) GET(path string, handlers ...HandlerFunc) IRoutes {
	return g.Handle(http.MethodGet, path, handlers...)
}
func (g *RouterGroup) POST(path string, handlers ...HandlerFunc) IRoutes {
	return g.Handle(http.MethodPost, path, handlers...)
}
func (g *RouterGroup) PUT(path string, handlers ...HandlerFunc) IRoutes {
	return g.Handle(http.MethodPut, path, handlers...)
}
func (g *RouterGroup) PATCH(path string, handlers ...HandlerFunc) IRoutes {
	return g.Handle(http.MethodPatch, path, handlers...)
}
func (g *RouterGroup) DELETE(path string, handlers ...HandlerFunc) IRoutes {
	return g.Handle(http.MethodDelete, path, handlers...)
}
func (g *RouterGroup) HEAD(path string, handlers ...HandlerFunc) IRoutes {
	return g.Handle(http.MethodHead, path, handlers...)
}
func (g *RouterGroup) OPTIONS(path string, handlers ...HandlerFunc) IRoutes {
	return g.Handle(http.MethodOptions, path, handlers...)
}
func (g *RouterGroup) Any(path string, handlers ...HandlerFunc) IRoutes {
	for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete, http.MethodHead, http.MethodOptions} {
		g.Handle(method, path, handlers...)
	}
	return g
}

type IRoutes interface{}

type Engine struct {
	RouterGroup
	routesMu sync.RWMutex
	routes   []route
	noRoute  []HandlerFunc
	noMethod []HandlerFunc
}

func New() *Engine {
	e := &Engine{}
	e.RouterGroup = RouterGroup{engine: e}
	return e
}

func Default() *Engine                                { return New() }
func (e *Engine) Use(handlers ...HandlerFunc) IRoutes { e.RouterGroup.Use(handlers...); return e }
func (e *Engine) NoRoute(handlers ...HandlerFunc)     { e.noRoute = handlers }
func (e *Engine) NoMethod(handlers ...HandlerFunc)    { e.noMethod = handlers }
func (e *Engine) SetTrustedProxies([]string) error    { return nil }
func (e *Engine) Handler() http.Handler               { return e }

func (e *Engine) Routes() []RouteInfo {
	e.routesMu.RLock()
	defer e.routesMu.RUnlock()
	out := make([]RouteInfo, 0, len(e.routes))
	for _, item := range e.routes {
		out = append(out, RouteInfo{Method: item.method, Path: item.pattern})
	}
	return out
}

func (e *Engine) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	e.routesMu.RLock()
	routeItem, params, methodMatched := e.match(r.Method, r.URL.Path)
	e.routesMu.RUnlock()
	writer := newResponseWriter(w)
	c := &Context{Request: r, Writer: writer, Params: params, engine: e, index: -1}
	if routeItem == nil {
		baseHandlers := append([]HandlerFunc(nil), e.RouterGroup.handlers...)
		if methodMatched && len(e.noMethod) > 0 {
			c.handlers = append(baseHandlers, e.noMethod...)
		} else if len(e.noRoute) > 0 {
			c.handlers = append(baseHandlers, e.noRoute...)
		} else {
			c.handlers = baseHandlers
			c.handlers = append(c.handlers, func(ctx *Context) {
				ctx.Writer.WriteHeader(http.StatusNotFound)
				ctx.Abort()
			})
		}
		if len(c.handlers) == 0 {
			writer.WriteHeader(http.StatusNotFound)
			return
		}
	} else {
		c.fullPath = routeItem.pattern
		c.handlers = routeItem.handlers
	}
	c.Next()
}

func (e *Engine) match(method, requestPath string) (*route, Params, bool) {
	methodMatched := false
	for i := range e.routes {
		item := &e.routes[i]
		params, ok := matchPath(item.pattern, requestPath)
		if !ok {
			continue
		}
		if item.method != method {
			if method == http.MethodHead && item.method == http.MethodGet {
				return item, params, true
			}
			methodMatched = true
			continue
		}
		return item, params, true
	}
	return nil, nil, methodMatched
}

func (e *Engine) ServeHTTPHandler() http.Handler { return e }

func SetMode(string) {}
func CustomRecovery(handler func(*Context, any)) HandlerFunc {
	return func(c *Context) {
		defer func() {
			if recovered := recover(); recovered != nil {
				handler(c, recovered)
				c.Abort()
			}
		}()
		c.Next()
	}
}

func CreateTestContext(w http.ResponseWriter) (*Context, *Engine) {
	e := New()
	return &Context{Writer: newResponseWriter(w), engine: e, index: -1}, e
}

func newResponseWriter(w http.ResponseWriter) ResponseWriter {
	return &responseWriter{ResponseWriter: w}
}

func joinPath(base, relative string) string {
	if base == "" {
		base = "/"
	}
	if relative == "" || relative == "/" {
		if base == "/" {
			return "/"
		}
		return strings.TrimSuffix(base, "/")
	}
	return "/" + strings.Trim(strings.TrimSuffix(base, "/")+"/"+strings.TrimPrefix(relative, "/"), "/")
}

func matchPath(pattern, requestPath string) (Params, bool) {
	patternParts := splitPath(pattern)
	requestParts := splitPath(requestPath)
	params := make(Params, 0)
	for i := 0; i < len(patternParts); i++ {
		part := patternParts[i]
		if strings.HasPrefix(part, "*") {
			params = append(params, Param{Key: strings.TrimPrefix(part, "*"), Value: strings.Join(requestParts[i:], "/")})
			return params, true
		}
		if i >= len(requestParts) {
			return nil, false
		}
		if strings.HasPrefix(part, ":") {
			params = append(params, Param{Key: strings.TrimPrefix(part, ":"), Value: requestParts[i]})
			continue
		}
		if part != requestParts[i] {
			return nil, false
		}
	}
	return params, len(patternParts) == len(requestParts)
}

func splitPath(value string) []string {
	value = strings.Trim(value, "/")
	if value == "" {
		return nil
	}
	return strings.Split(value, "/")
}

func firstValue(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func setTextValue(field reflect.Value, value string) error {
	switch field.Kind() {
	case reflect.String:
		field.SetString(value)
	case reflect.Bool:
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return err
		}
		field.SetBool(parsed)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		parsed, err := strconv.ParseInt(value, 10, field.Type().Bits())
		if err != nil {
			return err
		}
		field.SetInt(parsed)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		parsed, err := strconv.ParseUint(value, 10, field.Type().Bits())
		if err != nil {
			return err
		}
		field.SetUint(parsed)
	default:
		return fmt.Errorf("unsupported URI field type %s", field.Type())
	}
	return nil
}
