package op

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestUtils_validateTTL(t *testing.T) {
	assert.NoError(t, validateTTL(10))
	assert.NoError(t, validateTTL(0))
	assert.Error(t, validateTTL(-1))
}
