package management

import (
	"os"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/stdlibhttp"
)

func TestMain(m *testing.M) {
	web.SetMode(web.TestMode)
	os.Exit(m.Run())
}
