# stats

package to capture vital statistics about a golang process.

allows other packages to just import the package and start publishing stats
similar to how the github.com/deep-compute/log is imported and used (global context).

# Usage

## Basic Usage
The main program needs to inform the package about the statsd address and any prefixes to apply.
Make sure to call Init ! it does some other stuff like tracking runtime gc / proc stats etc .
It also makes all calls no-op when you want to disable stats generation.

```go
package main

import (
    "github.com/deep-compute/stats"
    "github.com/random/randompackage"
)

func main() {
    stats.SetAddress("localhost:8125")
    // if the host machine has host name node0-mac-pc
    // we want it to be node0-mac-pc.exampleprog0.<rest of stats>
    stats.SetHostnamedPrefix("exampleprog0")
    if e := stats.Init(); e != nil {
        panic(e)
    }

    randompackage.RandomFunc()

    // more documentation eventually
}
```

The package needs to just import and publish stats

```go
package randompackage

import (
    "fmt"
    "github.com/deep-compute/stats"
)

func RandomFunc() {
    // does not care about the prefix
    // but assume the main program above was used,
    // this results in node0-mac-pc.exampleprog0.randomfunc 
    stats.Incr("randomfunc", 1)
    fmt.Printf("Hello World!")
}
```

## Disabling statsd for some programs
The stats package looks for an environmental variable `DC_STATSD_DISABLE`.
```bash
DC_STATSD_DISABLE=1 go run main.go

// or export it
export DC_STATSD_DISABLE=1
go run main.go
unset DC_STATSD_DISABLE
```

## Locker stats
The stats package also has two Locker objects Mutex and RWMutex.
They capture stats about how much time was spent waiting to acquire lock.
As a future update, it also captures how much time the lock was enabled for.
