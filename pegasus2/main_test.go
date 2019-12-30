package pegasus2

import (
	"os"
	"testing"

	"github.com/XiaoMi/pegasus-go-client/pegalog"
)

func TestMain(m *testing.M) {
	pegalog.SetLogger(pegalog.StderrLogger)
	retc := m.Run()
	os.Exit(retc)
}
