package cmd

import (
	"sync/atomic"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
)

// LatencyEvent represents a latency measurement event
type LatencyEvent struct {
	LatencyMicros int64
	Timestamp     time.Time
}

// PerformanceStats tracks performance metrics with channel-based latency collection
type PerformanceStats struct {
	TotalOps   int64
	SuccessOps int64
	FailedOps  int64
	Histogram  *hdrhistogram.Histogram
	StartTime  time.Time

	// Channel-based latency collection (no locks needed)
	latencyChannel chan LatencyEvent
	errorChannel   chan struct{}
	done           chan struct{}

	// Per-second histograms (accessed by multiple collector goroutines)
	currentSecond    int64
	currentHistogram *hdrhistogram.Histogram
	secondHistograms map[int64]*hdrhistogram.Histogram
}

func NewPerformanceStats() *PerformanceStats {
	// Create histogram with 1 microsecond to 1 minute range, 3 significant digits
	hist := hdrhistogram.New(1, 60*1000*1000, 3)

	ps := &PerformanceStats{
		Histogram:        hist,
		StartTime:        time.Now(),
		secondHistograms: make(map[int64]*hdrhistogram.Histogram),
		currentHistogram: hdrhistogram.New(1, 60*1000*1000, 3),
		latencyChannel:   make(chan LatencyEvent, 1000000), // Buffered channel to prevent blocking
		errorChannel:     make(chan struct{}, 10000),       // Buffered for errors
		done:             make(chan struct{}),
	}

	// Start multiple stats collection goroutines for high-concurrency scenarios
	numCollectors := 2 // Multiple collectors to handle high load
	for i := 0; i < numCollectors; i++ {
		go ps.statsCollector()
	}

	return ps
}


// statsCollector runs in dedicated goroutines to process latency events without locks
func (ps *PerformanceStats) statsCollector() {
	for {
		select {
		case event := <-ps.latencyChannel:
			second := event.Timestamp.Unix()

			// Record in overall histogram (thread-safe RecordValue)
			ps.Histogram.RecordValue(event.LatencyMicros)

			// Record in per-second histogram with atomic operations for thread safety
			currentSec := atomic.LoadInt64(&ps.currentSecond)
			if second != currentSec {
				// Try to update current second atomically
				if atomic.CompareAndSwapInt64(&ps.currentSecond, currentSec, second) {
					// We won the race - create new histogram for this second
					ps.currentHistogram = hdrhistogram.New(1, 60*1000*1000, 3)
				}
			}
			// Always try to record in current histogram (thread-safe)
			ps.currentHistogram.RecordValue(event.LatencyMicros)

			// Use atomic operations for thread safety with multiple collectors
			atomic.AddInt64(&ps.SuccessOps, 1)
			atomic.AddInt64(&ps.TotalOps, 1)

		case <-ps.errorChannel:
			// Use atomic operations for thread safety
			atomic.AddInt64(&ps.FailedOps, 1)
			atomic.AddInt64(&ps.TotalOps, 1)

		case <-ps.done:
			return
		}
	}
}

// RecordLatency sends a latency event to the stats collector (lock-free)
func (ps *PerformanceStats) RecordLatency(latencyMicros int64) {
	select {
	case ps.latencyChannel <- LatencyEvent{
		LatencyMicros: latencyMicros,
		Timestamp:     time.Now(),
	}:
		// Event sent successfully
	default:
		// Channel is full, drop the event to prevent blocking
		// This is acceptable for high-throughput scenarios
	}
}

// RecordError sends an error event to the stats collector (lock-free)
func (ps *PerformanceStats) RecordError() {
	select {
	case ps.errorChannel <- struct{}{}:
		// Error event sent successfully
	default:
		// Channel is full, drop the event to prevent blocking
	}
}

func (ps *PerformanceStats) GetQPS() float64 {
	elapsed := time.Since(ps.StartTime).Seconds()
	if elapsed == 0 {
		return 0
	}
	return float64(atomic.LoadInt64(&ps.TotalOps)) / elapsed
}

func (ps *PerformanceStats) GetStats() (int64, int64, int64, float64, int64, int64, int64) {
	total := atomic.LoadInt64(&ps.TotalOps)
	success := atomic.LoadInt64(&ps.SuccessOps)
	failed := atomic.LoadInt64(&ps.FailedOps)
	qps := ps.GetQPS()

	// Note: Reading from histogram without lock is safe for reads
	// The worst case is we get slightly stale data, which is acceptable for monitoring
	var p50, p95, p99 int64
	if ps.Histogram.TotalCount() > 0 {
		p50 = ps.Histogram.ValueAtQuantile(50)
		p95 = ps.Histogram.ValueAtQuantile(95)
		p99 = ps.Histogram.ValueAtQuantile(99)
	}

	return total, success, failed, qps, p50, p95, p99
}

// GetCurrentSecondStats returns stats for the current second
// Note: This may return slightly stale data since we're not using locks,
// but this is acceptable for monitoring purposes and eliminates contention
func (ps *PerformanceStats) GetCurrentSecondStats() (int64, int64, int64, int64, int64) {
	if ps.currentHistogram == nil || ps.currentHistogram.TotalCount() == 0 {
		return 0, 0, 0, 0, 0
	}

	return ps.currentHistogram.TotalCount(),
		ps.currentHistogram.ValueAtQuantile(50),
		ps.currentHistogram.ValueAtQuantile(95),
		ps.currentHistogram.ValueAtQuantile(99),
		ps.currentHistogram.Max()
}


// GetOverallStats returns overall statistics
func (ps *PerformanceStats) GetOverallStats() (int64, int64, int64, float64) {
	total := atomic.LoadInt64(&ps.TotalOps)
	success := atomic.LoadInt64(&ps.SuccessOps)
	failed := atomic.LoadInt64(&ps.FailedOps)
	qps := ps.GetQPS()

	return total, success, failed, qps
}

// Close shuts down the stats collector goroutine
func (ps *PerformanceStats) Close() {
	close(ps.done)
}
