package retention

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// BatchCollapse is used to collapse multiple values from a similar batch (eg. same pubsub messages) into a single value
type BatchCollapse struct {
	Value interface{}
	Config
	IsCanceled bool
	lastExec   time.Time
	nextExec   time.Time
	ctxCancel  context.CancelFunc
	mutex      sync.Mutex
}

// Config contains the configuration needed to setup the BatchCollapse with `CreateBatchCollapse()`
type Config struct {
	// RetentionDuration is the duration the message should be kept in the queue if no further messages arrive
	RetentionDuration time.Duration
	// MaxDuration is the max duration until a execution occurs
	MaxDuration time.Duration
	// ExecuteFunc will be called with the Value if the value is set.
	ExecuteFunc func(interface{})
}

// CreateBatchCollapse creates a new instance of BatchCollapse with the help of the provided config
func CreateBatchCollapse(conf Config) *BatchCollapse {
	ctx, cancel := context.WithCancel(context.Background())
	bc := &BatchCollapse{
		Value:     nil,
		Config:    conf,
		lastExec:  time.Now(),
		nextExec:  time.Now().Add(conf.RetentionDuration),
		ctxCancel: cancel,
	}

	signalChannel := make(chan os.Signal, 2)
	signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-signalChannel
		switch sig {
		case os.Interrupt:
		case syscall.SIGTERM:
			bc.Cancel()
		}
	}()

	go bc.executeIfCompleted(ctx)

	return bc
}

// Collapse either collapses a value into a previous set value, or sets the value if nil
// the method also resets the internal timer for when to execute the collapsed batch
func (bc *BatchCollapse) Collapse(value interface{}) {
	bc.mutex.Lock()
	if bc.Value == nil {
		bc.Value = value
	}
	bc.nextExec = time.Now().Add(bc.RetentionDuration)
	bc.mutex.Unlock()
}

func (bc *BatchCollapse) executeIfCompleted(ctx context.Context) {
	doProcess := func() {
		// check if ready to execute method
		bc.mutex.Lock()
		if bc.Value != nil && bc.ExecuteFunc != nil &&
			(bc.nextExec.Before(time.Now()) || bc.lastExec.Add(bc.MaxDuration).Before(time.Now())) {
			bc.ExecuteFunc(bc.Value)
			bc.lastExec = time.Now()
			bc.nextExec = time.Now().Add(bc.RetentionDuration)
			bc.Value = nil
		}
		bc.mutex.Unlock()
	}

	for true {
		time.Sleep(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			log.Println("BatchCollapse: Context canceled - exiting loop")
			return
		default:
			doProcess()
		}
	}
}

// Cancel will cancel the context, and in effect kill of the loop for the struct, before calling the execute method one last time
// Should only be called on SIGTERM or when done with the struct instance.
func (bc *BatchCollapse) Cancel() {
	bc.ctxCancel()
	bc.mutex.Lock()
	if bc.Value != nil && bc.ExecuteFunc != nil {
		bc.ExecuteFunc(bc.Value)
	}
	bc.mutex.Unlock()
	bc.IsCanceled = true
}
