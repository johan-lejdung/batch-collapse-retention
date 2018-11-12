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
	Values     map[string]interface{}
	IsCanceled bool
	Config

	lastExec  time.Time
	nextExec  time.Time
	ctxCancel context.CancelFunc
	mutex     sync.Mutex
}

// Config contains the configuration needed to setup the BatchCollapse with `CreateBatchCollapse()`
type Config struct {
	// RetentionDuration is the duration the message should be kept in the queue if no further messages arrive
	RetentionDuration time.Duration
	// MaxDuration is the max duration until a execution occurs
	MaxDuration time.Duration
	// ExecuteFunc will be called with the Value if the value is set.
	ExecuteFunc      func(interface{})
	RegisterShutdown bool
}

// CreateBatchCollapse creates a new instance of BatchCollapse with the help of the provided config
func CreateBatchCollapse(conf Config) *BatchCollapse {
	ctx, cancel := context.WithCancel(context.Background())
	bc := &BatchCollapse{
		Values:    make(map[string]interface{}),
		Config:    conf,
		lastExec:  time.Now(),
		nextExec:  time.Now().Add(conf.RetentionDuration),
		ctxCancel: cancel,
	}

	if conf.RegisterShutdown {
		signalChannel := make(chan os.Signal, 2)
		signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM, syscall.SIGKILL)
		go func() {
			sig := <-signalChannel
			switch sig {
			case os.Interrupt,
				syscall.SIGKILL,
				syscall.SIGTERM:
				bc.Cancel()
			default:
				log.Printf("Other signal %v", sig)
			}

		}()
	}

	go bc.executeIfCompleted(ctx)

	return bc
}

// Collapse either collapses a value into a previous set value, or sets the value if nil
// the method also resets the internal timer for when to execute the collapsed batch
func (bc *BatchCollapse) Collapse(key string, value interface{}) {
	bc.mutex.Lock()
	if !bc.KeyExists(key) {
		bc.Values[key] = value
	}
	bc.nextExec = time.Now().Add(bc.RetentionDuration)
	bc.mutex.Unlock()
}

// KeyExists checks if a key exist or not
func (bc *BatchCollapse) KeyExists(key string) bool {
	_, ok := bc.Values[key]
	return ok
}

func (bc *BatchCollapse) executeIfCompleted(ctx context.Context) {
	for true {
		time.Sleep(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			log.Println("BatchCollapse: Context canceled - exiting loop")
			return
		default:
			bc.doProcess(false)
		}
	}
}

func (bc *BatchCollapse) doProcess(forceProcess bool) {
	bc.mutex.Lock()
	for key, value := range bc.Values {
		if bc.KeyExists(key) && bc.ExecuteFunc != nil &&
			(forceProcess || bc.nextExec.Before(time.Now()) || bc.lastExec.Add(bc.MaxDuration).Before(time.Now())) {
			bc.ExecuteFunc(value)
			bc.lastExec = time.Now()
			bc.nextExec = time.Now().Add(bc.RetentionDuration)
			delete(bc.Values, key)
		}
	}
	bc.mutex.Unlock()
}

// Cancel will cancel the context, and in effect kill of the loop for the struct, before calling the execute method one last time
// Should only be called on SIGTERM or when done with the struct instance.
func (bc *BatchCollapse) Cancel() {
	bc.ctxCancel()
	bc.doProcess(true)
	bc.IsCanceled = true
}
