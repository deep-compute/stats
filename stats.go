// stats package provides a mechanism to capture vital statistics and transmit
// to a StatsD server
package stats

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/deep-compute/abool"
	"github.com/quipo/statsd"
	"os"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"
)

const DISABLE_STATS = "DC_STATSD_DISABLE"

// IsDisabled checks if stats are disabled
// i.e. if DISABLE_STATS has been set
// one can set it by `export DC_STATSD_DISABLE=1` or any other value other than ""
func IsDisabled() bool {
	return os.Getenv(DISABLE_STATS) != ""
}

// IsEnabled checks if stats should be enabled
// i.e. DISABLE_STATS is not set
// one can unset it using `unset DC_STATSD_DISABLE`
func IsEnabled() bool {
	return os.Getenv(DISABLE_STATS) == ""
}

var DEFAULT_FLUSH_INTERVAL time.Duration = 1 * time.Second
var DEFAULT_RUNTIME_STATS_INTERVAL time.Duration = 1 * time.Second
var DEFAULT_PROC_STATS_INTERVAL time.Duration = 1 * time.Second
var regex_nonalphanum *regexp.Regexp = regexp.MustCompile("[^A-Za-z0-9]+")

// Mutex has the same behavior as sync.Mutex. In addition, it supports
//   measuring the amount time a thread is blocked waiting to acquire
//   the mutex and then submitting the same to stats
// TODO add an unlock that lets us calculate the time a lock is used for.
type Mutex struct {

	// the stats object. if nil, then global stats is used
	Stats *Stats

	// prefix to be used for the metric representing locking time
	Prefix string

	// the actual mutex
	sync.Mutex
}

func (p *Mutex) Lock() {
	st := time.Now()
	p.Mutex.Lock()
	p.Stats.PrecisionTiming(p.Prefix, time.Since(st))
}

// RWMutex has the same behavior as sync.RWMutex. In addition, it supports
//   measuring the amount time a thread is blocked waiting to acquire
//   the mutex and then submitting the same to stats
type RWMutex struct {

	// the stats object. if nil, then global stats is used
	Stats *Stats

	// prefix to be used for the metric representing locking time
	Prefix string

	// the actual mutex
	sync.RWMutex
}

func (p *RWMutex) Lock() {
	st := time.Now()
	p.RWMutex.Lock()
	p.Stats.PrecisionTiming(p.Prefix, time.Since(st))
}

func (p *RWMutex) RLock() {
	st := time.Now()
	p.RWMutex.RLock()
	p.Stats.PrecisionTiming(p.Prefix, time.Since(st))
}

// BulkStats represents a set of collected measurements which need to be
// submitted to the StatsServer. This abstraction is useful when stats are
// collected from a process that does not have direct access to the StatsD
// server
// TODO: Only a few types are supported. Need to support all.
type BulkStats struct {
	Timing map[string]int64 `json:"timing"`
	Gauge  map[string]int64 `json:"gauge"`
	Incr   map[string]int64 `json:"incr"`
	Decr   map[string]int64 `json:"decr"`
}

// Stats represents the abstraction that helps in collecting stats from a
// process. All stats submitted to it are buffered locally for performance
// and then flushed to the StatsD server periodically.
type Stats struct {
	// Duration of time after which the buffered stats must be flushed
	// to the StatsD server periodically
	FlushInterval time.Duration

	// Duration of time after which various runtime stats about the current
	// process are collected and submitted periodically
	RuntimeStatsInterval time.Duration

	// Duration of time after which stats from /proc for the current process
	// are collected and submitted periodically
	ProcStatsInterval time.Duration

	// The ip:port of the StatsD server (port is optional). If empty then
	// localhost and default port are assumed.
	Address string

	// Prefix used for each stats key in order to achieve namespacing in the
	// stats hierarchy
	Prefix string

	stats                    *statsd.StatsdBuffer
	isRuntimeThreadRunning   *abool.AtomicBool
	isProcStatsThreadRunning *abool.AtomicBool
}

// Global singleton stats object
var stats Stats

func (p *Stats) Init() error {
	return p.init()
}

func (p *Stats) init() error {
	// no stats should be sent
	p.isRuntimeThreadRunning = abool.New()
	p.isProcStatsThreadRunning = abool.New()

	if IsDisabled() {
		noopAllFunctions()
		return nil
	}

	if p.Prefix == "" {
		p.Prefix = GetDefaultPrefix()
	}

	if p.FlushInterval == 0 {
		p.FlushInterval = DEFAULT_FLUSH_INTERVAL
	}

	if p.RuntimeStatsInterval == 0 {
		p.RuntimeStatsInterval = DEFAULT_RUNTIME_STATS_INTERVAL
	}

	if p.ProcStatsInterval == 0 {
		p.ProcStatsInterval = DEFAULT_PROC_STATS_INTERVAL
	}

	if p.Address == "" {
		p.Address = ":8125"
	}

	stats := statsd.NewStatsdClient(p.Address, p.Prefix)
	e := stats.CreateSocket()
	if e != nil {
		return e
	}

	p.stats = statsd.NewStatsdBuffer(p.FlushInterval, stats)
	go p.collectRuntimeStats()
	go p.collectProcStats()
	return nil
}

// SetPrefix sets a new prefix and restarts connection
func (p *Stats) SetPrefix(prefix string) *Stats {
	if !strings.HasSuffix(prefix, ".") {
		prefix += "."
	}

	p.Prefix = prefix
	return p
}

// SetHostnamedPrefix sets a new prefix which is of the format
// <hostname>.<@prefix> and restarts connection
func (p *Stats) SetHostnamedPrefix(prefix string) *Stats {
	host, _ := os.Hostname()
	host = regex_nonalphanum.ReplaceAllString(host, "")

	var newPrefix string
	if host != "" {
		newPrefix = host
	}

	if prefix != "" {
		newPrefix += "." + prefix
		if !strings.HasSuffix(prefix, ".") {
			newPrefix += "."
		}
	}

	p.Prefix = newPrefix
	return p
}

// SetAddress accepts an address to StatsD server of the format
// ip:port (port is optional. default assumed if not present). if ip is not
// provided then localhost is assumed. New address is set and the connection
// is restarted
func (p *Stats) SetAddress(address string) *Stats {
	p.Address = address
	return p
}

func (p *Stats) collectRuntimeStats() {
	var m runtime.MemStats
	var nilCount int

	// If already running, then exit
	if !p.isRuntimeThreadRunning.TestAndSet() {
		return
	}

	for {
		// Terminate this goroutine if stats is in closed state
		// for significant period of time
		if p.stats == nil {
			nilCount++
			if nilCount >= 100 {
				p.isRuntimeThreadRunning.SetTo(false)
				break
			}
			continue
		}

		nilCount = 0

		// collect and submit runtime stats
		runtime.ReadMemStats(&m)

		g := p.Gauge

		g("runtime.num_goroutines", int64(runtime.NumGoroutine()))
		g("runtime.memory.alloc", int64(m.Alloc))            // in bytes
		g("runtime.memory.total_alloc", int64(m.TotalAlloc)) // in bytes
		g("runtime.memory.sys", int64(m.Sys))                // in bytes
		g("runtime.memory.lookups", int64(m.Lookups))        //num

		g("runtime.memory.heap.alloc", int64(m.HeapAlloc))       // in bytes
		g("runtime.memory.heap.sys", int64(m.HeapSys))           // in bytes
		g("runtime.memory.heap.idle", int64(m.HeapIdle))         // in bytes
		g("runtime.memory.heap.inuse", int64(m.HeapInuse))       // in bytes
		g("runtime.memory.heap.released", int64(m.HeapReleased)) // in bytes
		g("runtime.memory.heap.objects", int64(m.HeapObjects))   //num

		g("runtime.memory.mallocs", int64(m.Mallocs)) // num
		g("runtime.memory.frees", int64(m.Frees))     // num

		g("runtime.memory.stack.sys", int64(m.StackSys))     // bytes
		g("runtime.memory.stack.inuse", int64(m.StackInuse)) // bytes

		g("runtime.memory.gc.pause_total_ns", int64(m.PauseTotalNs)) //num nano seconds
		g("runtime.memory.gc.num_gc", int64(m.NumGC))                // num times gc has been called
		g("runtime.memory.gc.sys", int64(m.GCSys))                   // bytes

		g("runtime.memory.mcache.sys", int64(m.MCacheSys))     // bytes
		g("runtime.memory.mcache.inuse", int64(m.MCacheInuse)) // bytes

		g("runtime.memory.mspan.sys", int64(m.MSpanSys))     // bytes
		g("runtime.memory.mspan.inuse", int64(m.MSpanInuse)) // bytes

		g("runtime.memory.buckhash.sys", int64(m.BuckHashSys)) // bytes

		g("runtime.memory.other.sys", int64(m.OtherSys)) // bytes

		time.Sleep(p.RuntimeStatsInterval)
	}
}

// calculateMemory looks at the smaps for a pid and the res
// taken from http://stackoverflow.com/questions/31879817/golang-os-exec-realtime-memory-usage
func calculateMemory(pid int) (uint64, error) {

	f, err := os.Open(fmt.Sprintf("/proc/%d/smaps", pid))
	if err != nil {
		return 0, err
	}
	defer f.Close()

	res := uint64(0)
	pfx := []byte("Pss:")
	r := bufio.NewScanner(f)
	for r.Scan() {
		line := r.Bytes()
		if bytes.HasPrefix(line, pfx) {
			var size uint64
			_, err := fmt.Sscanf(string(line[4:]), "%d", &size)
			if err != nil {
				return 0, err
			}
			res += size
		}
	}
	if err := r.Err(); err != nil {
		return 0, err
	}

	return res, nil
}

// collectProcStats collects stats from /proc/pid for the process.
// TODO verify that pid is not pid of goroutine but that of process
func (p *Stats) collectProcStats() {
	var nilCount int

	// If already running, then exit
	if !p.isProcStatsThreadRunning.TestAndSet() {
		return
	}

	for {
		// Terminate this goroutine if stats is in closed state
		// for significant period of time
		if p.stats == nil {
			nilCount++
			if nilCount >= 100 {
				p.isProcStatsThreadRunning.SetTo(false)
				break
			}
			continue
		}
		nilCount = 0
		res, err := calculateMemory(os.Getpid())
		if err == nil {
			p.Gauge("proc.memory.res", int64(res))
		}
		time.Sleep(p.ProcStatsInterval)
	}
}

func (p *Stats) SubmitBulk(bstats *BulkStats) {
	for k, v := range bstats.Timing {
		p.Timing(k, v)
	}

	for k, v := range bstats.Gauge {
		p.Gauge(k, v)
	}

	for k, v := range bstats.Incr {
		p.Incr(k, v)
	}

	for k, v := range bstats.Decr {
		p.Decr(k, v)
	}
}

// NewMutex creates a new mutex (type stats.Mutex)
//	the @prefix is used to record lock time stats with stats server
func (p *Stats) NewMutex(prefix string) *Mutex {
	return &Mutex{Stats: p, Prefix: prefix}
}

// NewRWMutex creates a new mutex (type stats.RWMutex)
//	the @prefix is used to record lock time stats with stats server
func (p *Stats) NewRWMutex(prefix string) *RWMutex {
	return &RWMutex{Stats: p, Prefix: prefix}
}

// Following methods are wrappers over the underlying StatsdBuffer object
// For documentation please refer to the the library's docs.

func (p *Stats) Absolute(stat string, value int64) error {
	return p.stats.Absolute(stat, value)
}

func (p *Stats) Decr(stat string, count int64) error {
	return p.stats.Decr(stat, count)
}

func (p *Stats) FAbsolute(stat string, value float64) error {
	return p.stats.FAbsolute(stat, value)
}

func (p *Stats) FGauge(stat string, value float64) error {
	return p.stats.FGauge(stat, value)
}

func (p *Stats) FGaugeDelta(stat string, value float64) error {
	return p.stats.FGaugeDelta(stat, value)
}

func (p *Stats) Gauge(stat string, value int64) error {
	return p.stats.Gauge(stat, value)
}

func (p *Stats) GaugeDelta(stat string, value int64) error {
	return p.stats.GaugeDelta(stat, value)
}

func (p *Stats) Incr(stat string, count int64) error {
	return p.stats.Incr(stat, count)
}

func (p *Stats) PrecisionTiming(stat string, delta time.Duration) error {
	return p.stats.PrecisionTiming(stat, delta)
}

func (p *Stats) Timing(stat string, delta int64) error {
	return p.stats.Timing(stat, delta)
}

func (p *Stats) TimingSince(stat string, st time.Time) error {
	now := time.Now().UTC()
	return p.stats.PrecisionTiming(stat, now.Sub(st.UTC()))
}

func (p *Stats) Total(stat string, value int64) error {
	return p.stats.Total(stat, value)
}

// End of wrapper functions

// Following functions are package level easy access to the methods of the
// default global stats object

var Absolute = func(stat string, value int64) error {
	return stats.Absolute(stat, value)
}

var Decr = func(stat string, count int64) error {
	return stats.Decr(stat, count)
}

var FAbsolute = func(stat string, value float64) error {
	return stats.FAbsolute(stat, value)
}

var FGauge = func(stat string, value float64) error {
	return stats.FGauge(stat, value)
}

var FGaugeDelta = func(stat string, value float64) error {
	return stats.FGaugeDelta(stat, value)
}

var Gauge = func(stat string, value int64) error {
	return stats.Gauge(stat, value)
}

var GaugeDelta = func(stat string, value int64) error {
	return stats.GaugeDelta(stat, value)
}

var Incr = func(stat string, count int64) error {
	return stats.Incr(stat, count)
}

var PrecisionTiming = func(stat string, delta time.Duration) error {
	return stats.PrecisionTiming(stat, delta)
}

var Timing = func(stat string, delta int64) error {
	return stats.Timing(stat, delta)
}

var TimingSince = func(stat string, st time.Time) error {
	return stats.TimingSince(stat, st)
}

var Total = func(stat string, value int64) error {
	return stats.Total(stat, value)
}

var Noop = func(i, j interface{}) error {
	return nil
}

var NoopInt = func(key string, value int64) error {
	return nil
}

var NoopFloat = func(key string, value float64) error {
	return nil
}

var NoopDuration = func(key string, value time.Duration) error {
	return nil
}

var NoopTime = func(key string, value time.Time) error {
	return nil
}

func SetPrefix(prefix string) *Stats {
	return stats.SetPrefix(prefix)
}

func SetHostnamedPrefix(prefix string) *Stats {
	return stats.SetHostnamedPrefix(prefix)
}

func SetAddress(address string) *Stats {
	return stats.SetAddress(address)
}

func SubmitBulk(bstats *BulkStats) {
	stats.SubmitBulk(bstats)
}

type Locker interface {
	Lock()
	Unlock()
}

type RWLocker interface {
	RLock()
	RUnlock()
	Lock()
	Unlock()
}

var NewMutex = func(prefix string) Locker {
	if prefix != "" {
		prefix = fmt.Sprintf("mutex.%s", prefix)
	}
	return stats.NewMutex(prefix)
}

var NewRWMutex = func(prefix string) RWLocker {
	if prefix != "" {
		prefix = fmt.Sprintf("rwmutex.%s", prefix)
	}
	return stats.NewRWMutex(prefix)
}

var NoopMutex = func(prefix string) Locker {
	return &sync.Mutex{}
}
var NoopRWMutex = func(prefix string) RWLocker {
	return &sync.RWMutex{}
}

// End of package level easy access functions

// GetDefaultPrefix generates Stats prefix based on the hostname and the
// current process name in the format "<host>.<process>."
func GetDefaultPrefix() string {
	host, _ := os.Hostname()
	host = regex_nonalphanum.ReplaceAllString(host, "")
	process := regex_nonalphanum.ReplaceAllString(os.Args[0], "")

	var prefix string
	if host != "" {
		prefix = host
	}

	if process != "" {
		prefix += "." + process + "."
	}

	return prefix
}

// incase stats are disabled, noop all stat functions
func noopAllFunctions() {
	Absolute = NoopInt
	Decr = NoopInt
	FAbsolute = NoopFloat
	FGauge = NoopFloat
	FGaugeDelta = NoopFloat
	Gauge = NoopInt
	GaugeDelta = NoopInt
	Incr = NoopInt
	PrecisionTiming = NoopDuration
	Timing = NoopInt
	TimingSince = NoopTime
	Total = NoopInt
	NewMutex = NoopMutex
	NewRWMutex = NoopRWMutex
}

// DisableGlobalStats allows the user to disable all global level stats
// One may still create stats objects and use them - this is just for
// all the ones that rely on global level stats
func DisableGlobalStats() {
	noopAllFunctions()
}

func init() {
	statsd.UDPPayloadSize = 16 * 1024
}

// Init is called once all configurations have been set
//  e.g. stats.SetHostnamedPrefix
//  if e := stats.Init(); e != nil {
//   panic(e)
//  }
func Init() error {
	return stats.init()
}
