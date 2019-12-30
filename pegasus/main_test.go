package pegasus

import (
	"github.com/XiaoMi/pegasus-go-client/pegalog"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	pegalog.SetLogger(pegalog.StderrLogger)
	retc := m.Run()
	os.Exit(retc)
}
