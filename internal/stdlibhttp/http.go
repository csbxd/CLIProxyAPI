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

func (w *responseWriter) WriteHeaderNow() {
	status := w.status
	if status == 0 {
		status = http.StatusOK
	}
	w.WriteHeader(status)
}

func (w *responseWriter) Write(data []byte) (int, error) {
	if !w.written {
		w.WriteHeaderNow()
	}
	n, err := w.ResponseWriter.Write(data)
	w.size += n
	return n, err
}

func (w *responseWriter) WriteString(data string) (int, error) { return w.Write([]byte(data)) }

func (w *responseWriter) Flush() {
	if !w.written {
		w.WriteHeaderNow()
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

	engine       *Engine
	keys         map[string]any
	handlers     HandlersChain
	index        int8
	fullPath     string
	params       *Params
	skippedNodes *[]skippedNode
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

func (c *Context) FileFromFS(path string, fs http.FileSystem) {
	if c.Request == nil {
		return
	}
	oldPath := c.Request.URL.Path
	defer func() { c.Request.URL.Path = oldPath }()
	c.Request.URL.Path = path
	http.FileServer(fs).ServeHTTP(c.Writer, c.Request)
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

func newResponseWriter(w http.ResponseWriter) ResponseWriter {
	return &responseWriter{ResponseWriter: w}
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
