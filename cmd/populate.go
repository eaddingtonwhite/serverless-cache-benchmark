/*
Copyright © 2025 Redis Performance Group  <performance <at> redis <dot> com>
*/
package cmd

import (
	"context"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/time/rate"
)

// PopulateStats tracks populate operation statistics
type PopulateStats struct {
	TotalOps    int64
	SuccessOps  int64
	FailedOps   int64
	ActiveConns int64 // Current number of active connections
	StartTime   time.Time
	CSVLogger   *CSVLogger
	PerfStats   *PerformanceStats // Reference to performance stats for latency data
}

// CacheClient interface defines the operations for cache data sinks
type CacheClient interface {
	Set(ctx context.Context, key string, value []byte, expiration time.Duration) error
	Get(ctx context.Context, key string) ([]byte, error)
	Ping(ctx context.Context) error
	Close() error
	Name() string
}

// populateCmd represents the populate command
var populateCmd = &cobra.Command{
	Use:   "populate",
	Short: "Populate cache with test data using multiple concurrent clients",
	Long: `Populate cache systems (Redis or Momento) with test data for benchmarking using multiple concurrent clients.

This command supports populating both Redis and Momento cache systems with configurable
test data including different data sizes, key patterns, and expiration settings. It uses
multiple concurrent clients (goroutines) with optional rate limiting and provides real-time
performance metrics including QPS and per-second latency percentiles using HDR histogram.

Examples:
  # Populate Redis with default number of clients (4, optimized for I/O), unlimited rate
  serverless-cache-benchmark populate --cache-type redis --redis-uri redis://localhost:6379

  # Populate Redis with authentication and database selection
  serverless-cache-benchmark populate --cache-type redis --redis-uri redis://user:pass@localhost:6380/2

  # Populate Redis with TLS connection
  serverless-cache-benchmark populate --cache-type redis --redis-uri rediss://localhost:6380

  # Populate Momento with rate limiting at 1000 RPS total
  serverless-cache-benchmark populate --cache-type momento --momento-cache-name test-cache --rps 1000

  # Populate with 4 clients at 500 RPS (125 RPS per client)
  serverless-cache-benchmark populate --cache-type redis --redis-uri redis://localhost:6379 --clients 4 --rps 500

  # Populate with custom Redis timeouts for high-load scenarios
  serverless-cache-benchmark populate --cache-type redis --redis-dial-timeout 30 --redis-read-timeout 30 --redis-max-retries 5`,
	Run: runPopulate,
}

// ClientWorker represents a single client worker
type ClientWorker struct {
	ID        int
	Client    CacheClient
	Stats     *PerformanceStats
	Limiter   *rate.Limiter // Rate limiter for this client
	Generator *DataGenerator
	KeyStart  int
	KeyEnd    int
}

// workerRoutine runs a single client worker with rate limiting and channel-based stats
func (cw *ClientWorker) workerRoutine(ctx context.Context, wg *sync.WaitGroup, keyPrefix string, verbose bool, timeoutSeconds int, populateStats *PopulateStats) {
	defer wg.Done()
	defer cw.Client.Close()
	defer atomic.AddInt64(&populateStats.ActiveConns, -1) // Decrement when worker finishes

	var errorCount int64
	const maxErrorsToLog = 10            // Limit error logging to prevent spam
	errorSummary := make(map[string]int) // Track error types

	for i := cw.KeyStart; i <= cw.KeyEnd; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Apply rate limiting if configured
		if cw.Limiter != nil {
			err := cw.Limiter.Wait(ctx)
			if err != nil {
				if verbose && errorCount < maxErrorsToLog {
					log.Printf("Client %d: Rate limiter error: %v", cw.ID, err)
				}
				return
			}
		}

		key := fmt.Sprintf("%s%d", keyPrefix, i)

		data, err := cw.Generator.GenerateData()
		if err != nil {
			errorType := fmt.Sprintf("DataGen: %v", err)
			errorSummary[errorType]++
			if verbose && errorCount < maxErrorsToLog {
				log.Printf("Client %d: Failed to generate data for key %s: %v", cw.ID, key, err)
			} else if errorCount == maxErrorsToLog {
				log.Printf("Client %d: Suppressing further error logs. Error summary will be shown at end.", cw.ID)
			}
			cw.Stats.RecordError()
			atomic.AddInt64(&populateStats.FailedOps, 1)
			atomic.AddInt64(&populateStats.TotalOps, 1)
			errorCount++
			continue
		}

		expiration := cw.Generator.GetExpiration()

		// Create a timeout context for this operation
		opCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSeconds)*time.Second)
		start := time.Now()
		err = cw.Client.Set(opCtx, key, data, expiration)
		latency := time.Since(start)
		cancel()

		if err != nil {
			errorType := fmt.Sprintf("SET: %v", err)
			errorSummary[errorType]++
			if verbose && errorCount < maxErrorsToLog {
				log.Printf("Client %d: Failed to set key %s: %v", cw.ID, key, err)
			} else if errorCount == maxErrorsToLog {
				log.Printf("Client %d: Suppressing further error logs. Error summary will be shown at end.", cw.ID)
			}
			cw.Stats.RecordError()
			atomic.AddInt64(&populateStats.FailedOps, 1)
			atomic.AddInt64(&populateStats.TotalOps, 1)
			errorCount++
			continue
		}

		// Channel-based stats recording (lock-free, non-blocking)
		cw.Stats.RecordLatency(latency.Microseconds())
		atomic.AddInt64(&populateStats.SuccessOps, 1)
		atomic.AddInt64(&populateStats.TotalOps, 1)
	}

	// Log error summary if there were errors
	if errorCount > maxErrorsToLog && len(errorSummary) > 0 {
		log.Printf("Client %d: Error Summary (%d total errors):", cw.ID, errorCount)
		for errorType, count := range errorSummary {
			log.Printf("  %s: %d occurrences", errorType, count)
		}
	}
}

// createCacheClient creates a cache client based on the cache type
func createCacheClient(cacheType string, cmd *cobra.Command) (CacheClient, error) {
	switch cacheType {
	case "redis":
		uri, _ := cmd.Flags().GetString("redis-uri")
		clusterMode, _ := cmd.Flags().GetBool("cluster-mode")

		// Build Redis configuration from flags
		dialTimeout, _ := cmd.Flags().GetInt("redis-dial-timeout")
		readTimeout, _ := cmd.Flags().GetInt("redis-read-timeout")
		writeTimeout, _ := cmd.Flags().GetInt("redis-write-timeout")
		poolTimeout, _ := cmd.Flags().GetInt("redis-pool-timeout")
		connMaxIdleTime, _ := cmd.Flags().GetInt("redis-conn-max-idle-time")
		maxRetries, _ := cmd.Flags().GetInt("redis-max-retries")
		minRetryBackoff, _ := cmd.Flags().GetInt("redis-min-retry-backoff")
		maxRetryBackoff, _ := cmd.Flags().GetInt("redis-max-retry-backoff")

		config := RedisConfig{
			DialTimeout:     time.Duration(dialTimeout) * time.Second,
			ReadTimeout:     time.Duration(readTimeout) * time.Second,
			WriteTimeout:    time.Duration(writeTimeout) * time.Second,
			PoolTimeout:     time.Duration(poolTimeout) * time.Second,
			ConnMaxIdleTime: time.Duration(connMaxIdleTime) * time.Second,
			MaxRetries:      maxRetries,
			MinRetryBackoff: time.Duration(minRetryBackoff) * time.Millisecond,
			MaxRetryBackoff: time.Duration(maxRetryBackoff) * time.Millisecond,
			ClusterMode:     clusterMode,
		}

		client, err := NewRedisClientFromURI(uri, config)
		if err != nil {
			return nil, fmt.Errorf("failed to create Redis client from URI '%s': %w", uri, err)
		}
		return client, nil

	case "momento":
		apiKey, _ := cmd.Flags().GetString("momento-api-key")
		cacheName, _ := cmd.Flags().GetString("momento-cache-name")
		defaultTTL, _ := cmd.Flags().GetInt("default-ttl")
		clientConnCount, _ := cmd.Flags().GetUint32("momento-client-conn-count")

		// Don't create cache per worker - it should be created once upfront
		client, err := NewMomentoClient(apiKey, cacheName, false, defaultTTL, clientConnCount)
		if err != nil {
			return nil, fmt.Errorf("failed to create Momento client: %w", err)
		}
		return client, nil

	default:
		return nil, fmt.Errorf("invalid cache type: %s. Must be 'redis' or 'momento'", cacheType)
	}
}

// printStats prints performance statistics
func printStats(stats *PerformanceStats, clientCount int) {
	total, success, failed, qps, p50, p95, p99 := stats.GetStats()

	fmt.Printf("\n=== Performance Statistics ===\n")
	fmt.Printf("Clients: %d\n", clientCount)
	fmt.Printf("Total Operations: %d\n", total)
	fmt.Printf("Successful Operations: %d\n", success)
	fmt.Printf("Failed Operations: %d\n", failed)
	fmt.Printf("Success Rate: %.2f%%\n", float64(success)/float64(total)*100)
	fmt.Printf("QPS: %.2f\n", qps)
	fmt.Printf("Latency P50: %d μs (%.2f ms)\n", p50, float64(p50)/1000)
	fmt.Printf("Latency P95: %d μs (%.2f ms)\n", p95, float64(p95)/1000)
	fmt.Printf("Latency P99: %d μs (%.2f ms)\n", p99, float64(p99)/1000)
}

func runPopulate(cmd *cobra.Command, args []string) {
	// Start profiling if requested
	cpuProfile, _ := cmd.Flags().GetString("cpu-profile")
	memProfile, _ := cmd.Flags().GetString("mem-profile")
	blockProfile, _ := cmd.Flags().GetString("block-profile")
	mutexProfile, _ := cmd.Flags().GetString("mutex-profile")
	pprofAddr, _ := cmd.Flags().GetString("pprof-addr")
	blockProfileRate, _ := cmd.Flags().GetInt("block-profile-rate")
	mutexProfileFraction, _ := cmd.Flags().GetInt("mutex-profile-fraction")

	// Enable block profiling if requested
	if blockProfile != "" && blockProfileRate > 0 {
		runtime.SetBlockProfileRate(blockProfileRate)
		fmt.Printf("Block profiling enabled with rate: %d\n", blockProfileRate)

		if blockProfile != "" {
			defer func() {
				f, err := os.Create(blockProfile)
				if err != nil {
					log.Printf("Could not create block profile: %v", err)
					return
				}
				defer f.Close()

				if err := pprof.Lookup("block").WriteTo(f, 0); err != nil {
					log.Printf("Could not write block profile: %v", err)
				} else {
					fmt.Printf("Block profile written to: %s\n", blockProfile)
				}
			}()
		}
	}

	// Enable mutex profiling if requested
	if mutexProfile != "" && mutexProfileFraction > 0 {
		runtime.SetMutexProfileFraction(mutexProfileFraction)
		fmt.Printf("Mutex profiling enabled with fraction: %d\n", mutexProfileFraction)

		if mutexProfile != "" {
			defer func() {
				f, err := os.Create(mutexProfile)
				if err != nil {
					log.Printf("Could not create mutex profile: %v", err)
					return
				}
				defer f.Close()

				if err := pprof.Lookup("mutex").WriteTo(f, 0); err != nil {
					log.Printf("Could not write mutex profile: %v", err)
				} else {
					fmt.Printf("Mutex profile written to: %s\n", mutexProfile)
				}
			}()
		}
	}

	// Start pprof HTTP server if requested
	if pprofAddr != "" {
		go func() {
			fmt.Printf("Starting pprof HTTP server on http://%s/debug/pprof/\n", pprofAddr)
			if err := http.ListenAndServe(pprofAddr, nil); err != nil {
				log.Printf("pprof HTTP server failed: %v", err)
			}
		}()
	}

	if cpuProfile != "" {
		f, err := os.Create(cpuProfile)
		if err != nil {
			log.Fatalf("Could not create CPU profile: %v", err)
		}
		defer f.Close()

		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatalf("Could not start CPU profile: %v", err)
		}
		defer pprof.StopCPUProfile()
		fmt.Printf("CPU profiling enabled, writing to: %s\n", cpuProfile)
	}

	if memProfile != "" {
		defer func() {
			f, err := os.Create(memProfile)
			if err != nil {
				log.Printf("Could not create memory profile: %v", err)
				return
			}
			defer f.Close()

			runtime.GC() // Get up-to-date statistics
			if err := pprof.WriteHeapProfile(f); err != nil {
				log.Printf("Could not write memory profile: %v", err)
			} else {
				fmt.Printf("Memory profile written to: %s\n", memProfile)
			}
		}()
	}

	cacheType, _ := cmd.Flags().GetString("cache-type")
	clientCount, _ := cmd.Flags().GetInt("clients")
	rps, _ := cmd.Flags().GetInt("rps")
	timeoutSeconds, _ := cmd.Flags().GetInt("timeout")
	verbose, _ := cmd.Flags().GetBool("verbose")
	csvOutput, _ := cmd.Flags().GetString("csv-output")

	// Get populate parameters
	dataSize, _ := cmd.Flags().GetInt("data-size")
	randomData, _ := cmd.Flags().GetBool("random-data")
	dataSizeRange, _ := cmd.Flags().GetString("data-size-range")
	dataSizeList, _ := cmd.Flags().GetString("data-size-list")
	dataSizePattern, _ := cmd.Flags().GetString("data-size-pattern")
	expiryRange, _ := cmd.Flags().GetString("expiry-range")
	defaultTTL, _ := cmd.Flags().GetInt("default-ttl")
	keyPrefix, _ := cmd.Flags().GetString("key-prefix")
	keyMin, _ := cmd.Flags().GetInt("key-minimum")
	keyMax, _ := cmd.Flags().GetInt("key-maximum")

	// Validate parameters
	if clientCount <= 0 {
		log.Fatalf("Number of clients must be greater than 0")
	}

	totalKeys := keyMax - keyMin + 1
	if totalKeys <= 0 {
		log.Fatalf("Invalid key range: min=%d, max=%d", keyMin, keyMax)
	}

	// Calculate key ranges for each client
	keysPerClient := totalKeys / clientCount
	if keysPerClient == 0 {
		log.Fatalf("Not enough keys (%d) for %d clients", totalKeys, clientCount)
	}

	// Initialize CSV logging
	if csvOutput == "" {
		// Generate default filename with timestamp
		timestamp := time.Now().Format("20060102-150405")
		csvOutput = fmt.Sprintf("populate-%s-%s.csv", cacheType, timestamp)
	}

	csvLogger, err := NewCSVLogger(csvOutput)
	if err != nil {
		log.Fatalf("Failed to create CSV logger: %v", err)
	}
	defer csvLogger.Close()

	fmt.Printf("Starting population of %s cache...\n", cacheType)
	fmt.Printf("Clients: %d\n", clientCount)
	fmt.Printf("Total keys: %d (range: %d to %d)\n", totalKeys, keyMin, keyMax)
	fmt.Printf("Keys per client: %d\n", keysPerClient)
	fmt.Printf("Logging metrics to: %s\n", csvOutput)
	if rps > 0 {
		fmt.Printf("Rate limit: %d RPS total (%.2f RPS per client)\n", rps, float64(rps)/float64(clientCount))
	} else {
		fmt.Printf("Rate limit: unlimited\n")
	}
	fmt.Printf("Data size: %d bytes", dataSize)
	if dataSizeRange != "" {
		fmt.Printf(" (range: %s)", dataSizeRange)
	}
	fmt.Println()

	// For Momento, create cache once upfront to avoid multiple clients trying to create it
	if cacheType == "momento" {
		apiKey, _ := cmd.Flags().GetString("momento-api-key")
		cacheName, _ := cmd.Flags().GetString("momento-cache-name")
		createCache, _ := cmd.Flags().GetBool("momento-create-cache")
		clientConnCount, _ := cmd.Flags().GetUint32("momento-client-conn-count")

		if createCache {
			// Create a temporary client just to create the cache
			tempClient, err := NewMomentoClient(apiKey, cacheName, true, defaultTTL, clientConnCount)
			if err != nil {
				log.Fatalf("Failed to create Momento cache: %v", err)
			}
			tempClient.Close()
			fmt.Printf("Momento cache '%s' created/verified\n", cacheName)
		}
	}

	// Create shared performance stats
	perfStats := NewPerformanceStats()

	// Create populate stats for progress tracking
	populateStats := &PopulateStats{
		StartTime:   time.Now(),
		CSVLogger:   csvLogger,
		PerfStats:   perfStats,
		ActiveConns: int64(clientCount), // Start with all clients as active
	}

	// Create context for cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Handle signals in a separate goroutine
	go func() {
		<-sigChan
		fmt.Print("\r" + strings.Repeat(" ", 150) + "\r") // Clear progress line
		fmt.Println("\nReceived interrupt signal. Stopping workers and printing summary...")
		cancel() // Cancel context to stop all workers
	}()

	// Create workers
	var wg sync.WaitGroup
	workers := make([]*ClientWorker, clientCount)

	for i := 0; i < clientCount; i++ {
		// Create cache client for this worker
		client, err := createCacheClient(cacheType, cmd)
		if err != nil {
			log.Fatalf("Failed to create cache client for worker %d: %v", i, err)
		}

		// Calculate key range for this worker
		start := keyMin + i*keysPerClient
		end := start + keysPerClient - 1
		if i == clientCount-1 {
			// Last client gets any remaining keys
			end = keyMax
		}

		// Create data generator for this worker
		generator := &DataGenerator{
			DataSize:        dataSize,
			RandomData:      randomData,
			DataSizeRange:   dataSizeRange,
			DataSizeList:    dataSizeList,
			DataSizePattern: dataSizePattern,
			ExpiryRange:     expiryRange,
			DefaultTTL:      defaultTTL,
		}

		// Create rate limiter for this client if RPS is specified
		var limiter *rate.Limiter
		if rps > 0 {
			clientRPS := float64(rps) / float64(clientCount)
			limiter = rate.NewLimiter(rate.Limit(clientRPS), 1) // Burst of 1
		}

		workers[i] = &ClientWorker{
			ID:        i,
			Client:    client,
			Generator: generator,
			Stats:     perfStats,
			Limiter:   limiter,
			KeyStart:  start,
			KeyEnd:    end,
		}

		if verbose {
			fmt.Printf("Worker %d: keys %d to %d (%d keys)\n", i, start, end, end-start+1)
		}
	}

	// Start progress reporting with progress bar
	go reportPopulateProgress(ctx, populateStats, totalKeys, verbose)

	// Start all workers
	if verbose {
		fmt.Println("\nStarting workers...")
	} else {
		fmt.Printf("\nStarting %d workers...\n", clientCount)
	}

	for i, worker := range workers {
		wg.Add(1)
		go worker.workerRoutine(ctx, &wg, keyPrefix, verbose, timeoutSeconds, populateStats)
		if verbose {
			fmt.Printf("Started worker %d\n", i)
		}
	}

	// Wait for all workers to complete
	wg.Wait()

	// Clear progress line and close the stats collector
	fmt.Print("\r" + strings.Repeat(" ", 150) + "\r")
	perfStats.Close()

	// Print final statistics
	printStats(perfStats, clientCount)

	// Print CPU and system summary
	printSystemSummary()
}

func init() {
	rootCmd.AddCommand(populateCmd)

	// Cache Type Options
	populateCmd.Flags().StringP("cache-type", "t", "redis", "Cache type: redis or momento")

	// Client Options
	defaultClients := runtime.NumCPU()
	populateCmd.Flags().IntP("clients", "c", defaultClients, "Number of concurrent clients (default: 4, optimized for I/O)")
	populateCmd.Flags().IntP("rps", "r", 0, "Rate limit in requests per second (0 = unlimited)")
	populateCmd.Flags().IntP("timeout", "T", 10, "Operation timeout in seconds")
	populateCmd.Flags().BoolP("verbose", "v", false, "Enable verbose output (show worker details)")
	populateCmd.Flags().Int("default-ttl", 3600, "Default TTL in seconds for cache entries (0 = no expiration for Redis, 60s minimum for Momento)")
	populateCmd.Flags().String("csv-output", "", "CSV file to log populate metrics (default: auto-generated filename)")

	// Profiling Options
	populateCmd.Flags().String("cpu-profile", "", "Write CPU profile to file")
	populateCmd.Flags().String("mem-profile", "", "Write memory profile to file")
	populateCmd.Flags().String("block-profile", "", "Write block profile to file")
	populateCmd.Flags().String("mutex-profile", "", "Write mutex profile to file")
	populateCmd.Flags().String("pprof-addr", "", "Enable pprof HTTP server on address (e.g., localhost:6060)")
	populateCmd.Flags().Int("block-profile-rate", 1, "Block profile rate (0 = disabled, 1 = every blocking event)")
	populateCmd.Flags().Int("mutex-profile-fraction", 1, "Mutex profile fraction (0 = disabled, 1 = every mutex contention)")

	// Redis Options
	populateCmd.Flags().StringP("redis-uri", "u", "redis://localhost:6379", "Redis URI (redis://[username[:password]@]host[:port][/db-number] or rediss:// for TLS)")
	populateCmd.Flags().Bool("cluster-mode", false, "Run client in cluster mode")
	populateCmd.Flags().Int("redis-dial-timeout", 120, "Redis dial timeout in seconds")
	populateCmd.Flags().Int("redis-read-timeout", 120, "Redis read timeout in seconds")
	populateCmd.Flags().Int("redis-write-timeout", 120, "Redis write timeout in seconds")
	populateCmd.Flags().Int("redis-pool-timeout", 120, "Redis connection pool timeout in seconds")
	populateCmd.Flags().Int("redis-conn-max-idle-time", 120, "Redis connection max idle time in seconds")
	populateCmd.Flags().Int("redis-max-retries", 3, "Redis maximum number of retries")
	populateCmd.Flags().Int("redis-min-retry-backoff", 1000, "Redis minimum retry backoff in milliseconds")
	populateCmd.Flags().Int("redis-max-retry-backoff", 120000, "Redis maximum retry backoff in milliseconds")

	// Momento Options
	populateCmd.Flags().String("momento-api-key", "", "Momento API key (or set MOMENTO_API_KEY env var)")
	populateCmd.Flags().String("momento-cache-name", "test-cache", "Momento cache name")
	populateCmd.Flags().Bool("momento-create-cache", true, "Automatically create Momento cache if it doesn't exist")

	// Object Options
	populateCmd.Flags().IntP("data-size", "d", 32, "Object data size in bytes")
	populateCmd.Flags().BoolP("random-data", "R", false, "Indicate that data should be randomized")
	populateCmd.Flags().String("data-size-range", "", "Use random-sized items in the specified range (min-max)")
	populateCmd.Flags().String("data-size-list", "", "Use sizes from weight list (size1:weight1,..sizeN:weightN)")
	populateCmd.Flags().String("data-size-pattern", "R", "Use together with data-size-range (R=random, S=evenly distributed)")
	populateCmd.Flags().String("expiry-range", "", "Use random expiry values from the specified range")

	// Key Options
	populateCmd.Flags().String("key-prefix", "memtier-", "Prefix for keys")
	populateCmd.Flags().Int("key-minimum", 0, "Key ID minimum value")
	populateCmd.Flags().Int("key-maximum", 10000000, "Key ID maximum value")
}

// reportPopulateProgress reports populate progress with progress bar and system monitoring
func reportPopulateProgress(ctx context.Context, stats *PopulateStats, totalKeys int, verbose bool) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Clear the progress line
			fmt.Print("\r" + strings.Repeat(" ", 150) + "\r")
			return
		case <-ticker.C:
			totalOps := atomic.LoadInt64(&stats.TotalOps)
			successOps := atomic.LoadInt64(&stats.SuccessOps)
			failedOps := atomic.LoadInt64(&stats.FailedOps)

			elapsed := time.Since(stats.StartTime)

			if totalOps > 0 {
				qps := float64(totalOps) / elapsed.Seconds()
				successRate := float64(successOps) / float64(totalOps) * 100

				// Create progress bar based on completion
				progress := float64(totalOps) / float64(totalKeys)
				if progress > 1.0 {
					progress = 1.0
				}
				progressBar := createPopulateProgressBar(progress, elapsed)

				// Get system resource usage
				sysStats := getSystemStats()
				procMemMB := getProcessMemoryMB()

				// Format the progress line with resource monitoring
				progressLine := fmt.Sprintf("\r%s | %.0f ops/s | Success: %d (%.1f%%) | Failed: %d | Mem: %.1fGB/%.1fGB | CPU: %.0f%% | Proc: %.1fGB | Net: %.1f/%.1f MB/s",
					progressBar, qps, successOps, successRate, failedOps,
					sysStats.MemoryUsedMB/1024, sysStats.MemoryTotalMB/1024,
					sysStats.CPUPercent, procMemMB/1024, sysStats.NetworkRxMBps, sysStats.NetworkTxMBps)

				// Truncate if too long for terminal
				if len(progressLine) > 150 {
					progressLine = progressLine[:147] + "..."
				}

				fmt.Print(progressLine)

				// Log to CSV if available
				if stats.CSVLogger != nil {
					// Get latency metrics from performance stats
					_, _, _, _, p50, p95, p99 := stats.PerfStats.GetStats()

					snapshot := MetricsSnapshot{
						Timestamp:         time.Now(),
						ElapsedSeconds:    int(elapsed.Seconds()),
						TargetClients:     0,  // Not applicable for populate
						ActualClients:     0,  // Not applicable for populate
						TargetQPS:         -1, // Not applicable for populate
						ActualTotalQPS:    qps,
						ActualGetQPS:      0, // Only SET operations in populate
						ActualSetQPS:      qps,
						TotalOps:          totalOps,
						GetOps:            0,
						SetOps:            successOps,
						GetErrors:         0,
						SetErrors:         failedOps,
						GetLatencyP50:     0, // GET latencies not applicable
						GetLatencyP95:     0,
						GetLatencyP99:     0,
						GetLatencyMax:     0,
						SetLatencyP50:     p50, // Use performance stats for SET latencies
						SetLatencyP95:     p95,
						SetLatencyP99:     p99,
						SetLatencyMax:     stats.PerfStats.Histogram.Max(),
						NetworkRxMBps:     sysStats.NetworkRxMBps,
						NetworkTxMBps:     sysStats.NetworkTxMBps,
						NetworkRxPPS:      sysStats.NetworkRxPPS,
						NetworkTxPPS:      sysStats.NetworkTxPPS,
						MemoryUsedGB:      sysStats.MemoryUsedMB / 1024,
						MemoryTotalGB:     sysStats.MemoryTotalMB / 1024,
						CPUPercent:        sysStats.CPUPercent,
						ProcessMemoryGB:   procMemMB / 1024,
						TotalOutBoundConn: sysStats.OutboundTCPConns,
					}
					stats.CSVLogger.LogMetrics(snapshot)
				}
			}
		}
	}
}

// createPopulateProgressBar creates a progress bar for populate operations
func createPopulateProgressBar(progress float64, elapsed time.Duration) string {
	barWidth := 20
	filled := int(progress * float64(barWidth))

	bar := "["
	for i := 0; i < barWidth; i++ {
		if i < filled {
			bar += "="
		} else if i == filled && progress < 1.0 {
			bar += ">"
		} else {
			bar += "-"
		}
	}
	bar += "]"

	// Add elapsed time and percentage
	minutes := int(elapsed.Minutes())
	seconds := int(elapsed.Seconds()) % 60
	timeStr := fmt.Sprintf("%02d:%02d", minutes, seconds)

	return fmt.Sprintf("%s %s (%.1f%%)", bar, timeStr, progress*100)
}

// printSystemSummary prints CPU and system resource summary
func printSystemSummary() {
	fmt.Printf("\n=== System Resource Summary ===\n")

	// Get current system stats
	sysStats := getSystemStats()
	procMemMB := getProcessMemoryMB()

	// Get CPU count
	cpuCount := runtime.NumCPU()

	// Get Go runtime stats
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	fmt.Printf("CPU Information:\n")
	fmt.Printf("  CPU Cores: %d\n", cpuCount)
	fmt.Printf("  Current CPU Usage: %.1f%%\n", sysStats.CPUPercent)
	fmt.Println()

	fmt.Printf("Memory Information:\n")
	fmt.Printf("  System Memory: %.2f GB used / %.2f GB total (%.1f%% used)\n",
		sysStats.MemoryUsedMB/1024, sysStats.MemoryTotalMB/1024,
		(sysStats.MemoryUsedMB/sysStats.MemoryTotalMB)*100)
	fmt.Printf("  Process Memory: %.2f GB\n", procMemMB/1024)
	fmt.Println()

	fmt.Printf("Go Runtime Information:\n")
	fmt.Printf("  Goroutines: %d\n", runtime.NumGoroutine())
	fmt.Printf("  Heap Allocated: %.2f MB\n", float64(memStats.Alloc)/1024/1024)
	fmt.Printf("  Heap System: %.2f MB\n", float64(memStats.HeapSys)/1024/1024)
	fmt.Printf("  GC Cycles: %d\n", memStats.NumGC)
	fmt.Printf("  Last GC: %v ago\n", time.Since(time.Unix(0, int64(memStats.LastGC))))

	// Network summary if available
	if sysStats.NetworkRxMBps > 0 || sysStats.NetworkTxMBps > 0 {
		fmt.Println()
		fmt.Printf("Network Information:\n")
		fmt.Printf("  Network RX: %.3f MB/s (%.0f packets/s)\n", sysStats.NetworkRxMBps, sysStats.NetworkRxPPS)
		fmt.Printf("  Network TX: %.3f MB/s (%.0f packets/s)\n", sysStats.NetworkTxMBps, sysStats.NetworkTxPPS)
	}

	fmt.Println()
}
