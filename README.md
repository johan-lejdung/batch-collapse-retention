# batch-collapse-retention
A small package made to batch together - async - results, processes and queues. Listens to SIGTERM

Useful when you want to collapse multiple values or messages into a single one. I made it to group pubsub messages together, and then just send a single one.


# Install

```
go get github.com/johan-lejdung/batch-collapse-retention
```

# Usage

Import the package
```
import retention github.com/johan-lejdung/batch-collapse-retention
```

Create a new instance of the struct with:
```
bc := retention.CreateBatchCollapse(retention.Config{
    RetentionDuration: 5 * time.Second,
    MaxDuration:       60 * time.Second,
    ExecuteFunc: func(value interface{}) {
        log.Printf("Executing function with value %v", value)
    },
})
```

Every time you collapse a value via `Collapse()` the `RetentionDuration` will reset, but the time will never pass `MaxDuration` between two executions.
