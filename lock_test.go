package dyno

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLock(t *testing.T) {
	lock1 := NewLock(testClient, tableName, "PK", "SK", "testing-lock")
	lock2 := NewLock(testClient, tableName, "PK", "SK", "testing-lock")

	t.Run("Acquire", func(t *testing.T) {
		t.Run("given a locked lock", func(t *testing.T) {
			t.Log("lock1.Acquire")
			err := lock1.Acquire(time.Duration(30 * time.Second))
			require.NoError(t, err)

			t.Log("lock2.Acquire")
			err = lock2.Acquire(time.Duration(30 * time.Second))
			assert.Error(t, err)

			t.Log("lock1.Release")
			err = lock1.Release()
			require.NoError(t, err)

			t.Log("lock2.Acquire")
			err = lock1.Acquire(time.Duration(30 * time.Second))
			assert.NoError(t, err)

			t.Log("lock2.Release")
			lock2.Release()
		})
	})
}
