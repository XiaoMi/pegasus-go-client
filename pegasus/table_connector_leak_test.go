package pegasus

import (
	"context"
	"fmt"
	"testing"

	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/assert"
)

func TestPegasusTableConnector_CloseMustNotLeak(t *testing.T) {
	defer leaktest.Check(t)()

	for i := 0; i < 20; i++ {
		client := NewClient(testingCfg)
		defer client.Close()

		tb, err := client.OpenTable(context.Background(), "temp")
		assert.Nil(t, err)
		defer tb.Close()

		_, err = tb.Get(context.Background(), []byte(fmt.Sprintf("%d", i)), []byte("sortkey"))
		assert.Nil(t, err)
	}
}
