package retention_test

import (
	"log"
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
	bc := retention.CreateBatchCollapse(retention.Config{
		RetentionDuration: 5 * time.Millisecond,
		MaxDuration:       60 * time.Second,
		ExecuteFunc: func(value interface{}) {
			testInt = intPtr(11)
			log.Printf("Executing function with value %v", value)
		},
	})

	assert.Equal(t, 10, *testInt)
	bc.Collapse(10)

	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, 11, *testInt)
}

func TestExecMulti(t *testing.T) {
	var testInt *int
	testInt = intPtr(10)
	bc := retention.CreateBatchCollapse(retention.Config{
		RetentionDuration: 5 * time.Millisecond,
		MaxDuration:       60 * time.Second,
		ExecuteFunc: func(value interface{}) {
			testInt = intPtr(11)
			log.Printf("Executing function with value %v", value)
		},
	})

	assert.Equal(t, 10, *testInt)
	bc.Collapse(10)
	bc.Collapse(10)
	bc.Collapse(10)

	time.Sleep(5 * time.Millisecond)
	assert.Equal(t, 10, *testInt)

	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, 11, *testInt)
}

func intPtr(i int) *int {
	return &i
}

func TestCancel__WithExec(t *testing.T) {
	var testInt *int
	testInt = intPtr(10)
	bc := retention.CreateBatchCollapse(retention.Config{
		RetentionDuration: 5 * time.Second,
		MaxDuration:       60 * time.Second,
		ExecuteFunc: func(value interface{}) {
			testInt = intPtr(11)
			log.Printf("Executing function with value %v", value)
		},
	})

	assert.Equal(t, 10, *testInt)
	bc.Collapse(10)

	assert.False(t, bc.IsCanceled)
	bc.Cancel()
	assert.True(t, bc.IsCanceled)

	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, 11, *testInt)
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
