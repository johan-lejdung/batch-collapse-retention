package retention_test

import (
	"log"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	retention "github.com/johan-lejdung/batch-collapse-retention"
)

func TestCollapse(t *testing.T) {
	bc := retention.CreateBatchCollapse(retention.Config{
		RetentionDuration: 5 * time.Second,
		MaxDuration:       60 * time.Second,
		ExecuteFunc: func(value interface{}) {
			log.Printf("Executing function with value %v", value)
		},
	})

	assert.Nil(t, bc.Value)
	bc.Collapse(10)
	assert.NotNil(t, bc.Value)
	assert.Equal(t, 10, bc.Value)
}

func TestExec(t *testing.T) {
	var testInt *int
	testInt = intPtr(10)
	var mutex sync.Mutex
	bc := retention.CreateBatchCollapse(retention.Config{
		RetentionDuration: 1 * time.Millisecond,
		MaxDuration:       60 * time.Second,
		ExecuteFunc: func(value interface{}) {
			mutex.Lock()
			testInt = intPtr(11)
			mutex.Unlock()
			log.Printf("Executing function with value %v", value)
		},
	})

	mutex.Lock()
	assert.Equal(t, 10, *testInt)
	mutex.Unlock()
	bc.Collapse(10)

	time.Sleep(15 * time.Millisecond)
	mutex.Lock()
	assert.Equal(t, 11, *testInt)
	mutex.Unlock()
}

func TestExecMulti(t *testing.T) {
	var testInt *int
	testInt = intPtr(10)
	var mutex sync.Mutex
	bc := retention.CreateBatchCollapse(retention.Config{
		RetentionDuration: 5 * time.Millisecond,
		MaxDuration:       60 * time.Second,
		ExecuteFunc: func(value interface{}) {
			mutex.Lock()
			testInt = intPtr(11)
			mutex.Unlock()
			log.Printf("Executing function with value %v", value)
		},
	})

	mutex.Lock()
	assert.Equal(t, 10, *testInt)
	mutex.Unlock()
	bc.Collapse(10)
	bc.Collapse(10)
	bc.Collapse(10)

	mutex.Lock()
	assert.Equal(t, 10, *testInt)
	mutex.Unlock()

	time.Sleep(15 * time.Millisecond)
	mutex.Lock()
	assert.Equal(t, 11, *testInt)
	mutex.Unlock()
}

func intPtr(i int) *int {
	return &i
}

func TestCancel__WithExec(t *testing.T) {
	var testInt *int
	testInt = intPtr(10)
	var mutex sync.Mutex
	bc := retention.CreateBatchCollapse(retention.Config{
		RetentionDuration: 5 * time.Second,
		MaxDuration:       60 * time.Second,
		ExecuteFunc: func(value interface{}) {
			mutex.Lock()
			testInt = intPtr(11)
			mutex.Unlock()
			log.Printf("Executing function with value %v", value)
		},
	})

	mutex.Lock()
	assert.Equal(t, 10, *testInt)
	mutex.Unlock()
	bc.Collapse(10)

	assert.False(t, bc.IsCanceled)
	bc.Cancel()
	assert.True(t, bc.IsCanceled)

	time.Sleep(10 * time.Millisecond)
	mutex.Lock()
	assert.Equal(t, 11, *testInt)
	mutex.Unlock()
}

func TestCancel__WithoutExec(t *testing.T) {
	bc := retention.CreateBatchCollapse(retention.Config{
		RetentionDuration: 5 * time.Second,
		MaxDuration:       60 * time.Second,
		ExecuteFunc: func(value interface{}) {
			log.Printf("Executing function with value %v", value)
		},
	})

	assert.False(t, bc.IsCanceled)
	bc.Cancel()
	assert.True(t, bc.IsCanceled)
}
