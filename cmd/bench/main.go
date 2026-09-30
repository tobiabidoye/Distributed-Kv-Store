package main

import (
	"flag"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tobiabidoye/distributed-raft/kv"
	"github.com/tobiabidoye/distributed-raft/kvrpc"
)

type LatencySlice []time.Duration

func (s LatencySlice) Len() int           { return len(s) }
func (s LatencySlice) Less(i, j int) bool { return s[i] < s[j] }
func (s LatencySlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// get latency for each percentile
// p99, p50 so on and so forth
func percentile(sortedSlice LatencySlice, percentage float64) time.Duration {
	if len(sortedSlice) == 0 {
		return 0
	}

	idx := int(float64(len(sortedSlice)-1) * (percentage / 100.0))
	return sortedSlice[idx]
}

func main() {
	serversFlag := flag.String("servers", "", "comma separated server endpoints")
	duration := flag.Duration("duration", 10*time.Second, "Duration of the benchmark run")
	concurrency := flag.Int("concurrency", 16, "Number of concurrent worker goroutines")
	workload := flag.String("workload", "put", "Workload type: put, get, mixed (80/20 read/write)")
	valSize := flag.Int("val-size", 64, "Payload value size in bytes")
	flag.Parse()

	if *serversFlag == "" {
		fmt.Println("Error servers flag is required")
		return
	}

	servers := strings.Split(*serversFlag, ",")
	valPayload := strings.Repeat("x", *valSize)

	//prepopulate keys if gets are involved with benchmark
	if *workload == "get" || *workload == "mixed" {
		fmt.Println("Prepopulating 1000 keys for reading")
		prepClerk := kv.MakeClerk(servers)
		//puts to the existing kv servers
		for i := 0; i < 1000; i++ {
			key := fmt.Sprintf("bench-key-%d", i)
			prepClerk.Put(key, valPayload, 0)
		}
		fmt.Println("finished prepopulating kv servers")
	}

	var totalOps int64
	var totalErrors int64
	//each bucket is for one of the concurrent workers essentially
	workerLatencies := make([][]time.Duration, *concurrency)
	stopChan := make(chan struct{})
	var wg sync.WaitGroup

	fmt.Printf("\n=== Starting %s Benchmark ===\n", strings.ToUpper(*workload))
	fmt.Printf("Duration: %v | Concurrency: %d workers | Payload: %d bytes\n", *duration, *concurrency, *valSize)
	startTime := time.Now()
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		workerId := i
		go func(id int) {
			defer wg.Done()
			clerk := kv.MakeClerk(servers)
			latencies := make([]time.Duration, 0, 5000)
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))

			var keyVersion map[string]int
			keyVersion = make(map[string]int)
			for {
				select {
				case <-stopChan:
					workerLatencies[id] = latencies
					return
				default:
					key := fmt.Sprintf("bench-key-%d-%d", r.Intn(1000), id)
					version := 0
					if _, ok := keyVersion[key]; ok {
						version = keyVersion[key]
					}
					t0 := time.Now()
					var err kvrpc.Err
					switch *workload {
					case "put":
						err = clerk.Put(key, valPayload, kvrpc.Tversion(version))
						version += 1
						keyVersion[key] = version
					case "get":
						_, _, err = clerk.Get(key)
					case "mixed":
						if r.Float64() < 0.80 {
							_, _, err = clerk.Get(key)
						} else {
							err = clerk.Put(key, valPayload, kvrpc.Tversion(version))
							version += 1
							keyVersion[key] = version
						}
					}

					elapsed := time.Since(t0)
					if err != kvrpc.OK {
						atomic.AddInt64(&totalErrors, 1)
					} else {
						latencies = append(latencies, elapsed)
						atomic.AddInt64(&totalOps, 1)
					}
				}
			}
		}(workerId)
	}

	time.Sleep(*duration)
	close(stopChan)
	wg.Wait()
	totalTime := time.Since(startTime)
	//sort latencies for percentiles to be calculated
	var allLatencies LatencySlice
	for _, l := range workerLatencies {
		allLatencies = append(allLatencies, l...)
	}

	sort.Sort(allLatencies)
	throughput := float64(totalOps) / totalTime.Seconds()

	fmt.Println("\n================ Benchmark Results ================")
	fmt.Printf("Total Successful Ops: %d\n", totalOps)
	fmt.Printf("Total Errors / Drops: %d\n", totalErrors)
	fmt.Printf("Total Wall Time:      %v\n", totalTime.Round(time.Millisecond))
	fmt.Printf("Throughput:           %.2f ops/sec\n", throughput)
	if len(allLatencies) > 0 {
		fmt.Printf("Latency (p50):        %v\n", percentile(allLatencies, 50))
		fmt.Printf("Latency (p95):        %v\n", percentile(allLatencies, 95))
		fmt.Printf("Latency (p99):        %v\n", percentile(allLatencies, 99))
		fmt.Printf("Latency (Max):        %v\n", allLatencies[len(allLatencies)-1])
	}
	fmt.Println("===================================================")
}
