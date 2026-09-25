package kv

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/tobiabidoye/distributed-raft/cmd/util"
	"github.com/tobiabidoye/distributed-raft/kvrpc"
)

// this function was written by an llm, did not feel like it was worth spending the time on it
func TestPorcupineLinearizability(t *testing.T) {
	baseDir := t.TempDir()
	const numServers = 3
	const numClients = 4
	const opsPerClient = 25

	stopFuncs := make([]func(), numServers)
	defer func() {
		for _, stop := range stopFuncs {
			if stop != nil {
				stop()
			}
		}
	}()

	for i := range numServers {
		_, stopFuncs[i] = StartTestKvServer(t, i, baseDir, numServers, -1)
	}

	ports := util.DynamicPorts(numServers)

	var opsMu sync.Mutex
	var operations []porcupine.Operation

	var wg sync.WaitGroup
	keys := []string{"key_a", "key_b"}

	for clientId := range numClients {
		wg.Add(1)
		go func(cid int) {
			defer wg.Done()
			clerk := MakeClerk(ports)
			r := rand.New(rand.NewSource(int64(cid + 100)))

			for j := range opsPerClient {
				key := keys[r.Intn(len(keys))]
				isPut := r.Float32() < 0.6

				if isPut {
					_, curVer, errGet := clerk.Get(key)
					reqVersion := uint64(curVer)
					if errGet == kvrpc.ErrNoKey {
						reqVersion = 0
					}

					val := fmt.Sprintf("val_%d_%d", cid, j)

					callTime := time.Now().UnixNano()
					err := clerk.Put(key, val, kvrpc.Tversion(reqVersion))
					returnTime := time.Now().UnixNano()

					errStr := "OK"
					if err == kvrpc.ErrVersion {
						errStr = "ErrVersion"
					} else if err != kvrpc.OK {
						errStr = string(err)
					}

					op := porcupine.Operation{
						ClientId: cid,
						Input:    KvInput{Op: "Put", Key: key, Value: val, Version: reqVersion},
						Output:   KvOutput{Err: errStr},
						Call:     callTime,
						Return:   returnTime,
					}

					opsMu.Lock()
					operations = append(operations, op)
					opsMu.Unlock()
				} else {
					callTime := time.Now().UnixNano()
					val, ver, err := clerk.Get(key)
					returnTime := time.Now().UnixNano()

					errStr := "OK"
					if err == kvrpc.ErrNoKey {
						errStr = "ErrNoKey"
					} else if err != kvrpc.OK {
						errStr = string(err)
					}

					op := porcupine.Operation{
						ClientId: cid,
						Input:    KvInput{Op: "Get", Key: key},
						Output:   KvOutput{Value: val, Version: uint64(ver), Err: errStr},
						Call:     callTime,
						Return:   returnTime,
					}

					opsMu.Lock()
					operations = append(operations, op)
					opsMu.Unlock()
				}
			}
		}(clientId)
	}

	wg.Wait()

	t.Logf("Collected %d concurrent operations. Verifying with Porcupine...", len(operations))

	// Step 3: Check linearizability!
	res, info := porcupine.CheckOperationsVerbose(kvModel, operations, 0)
	if res != porcupine.Ok {
		porcupine.VisualizePath(kvModel, info, "linearizability_violation.html")
		t.Fatalf("history is not linearizable! Visualization written to linearizability_violation.html")
	}

	t.Log("PASS: History is provably linearizable!")
}
