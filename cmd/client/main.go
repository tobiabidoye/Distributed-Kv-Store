package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/tobiabidoye/distributed-raft/kv"
	"github.com/tobiabidoye/distributed-raft/kvrpc"
)

func printHelp() {
	fmt.Println("\nAvailable commands:")
	fmt.Println("  get <key>                   - Fetch value and version")
	fmt.Println("  put <key> <val> [version]   - Write value (version 0 creates/blind-writes)")
	fmt.Println("  leader                      - Show current suspected leader index")
	fmt.Println("  help                        - Show commands")
	fmt.Println("  exit                        - Quit CLI")
}

func main() {
	serversFlag := flag.String("servers", "127.0.0.1:8000,127.0.0.1:8001,127.0.0.1:8002", "Comma-separated peer endpoints")
	flag.Parse()

	servers := strings.Split(*serversFlag, ",")
	clerk := kv.MakeClerk(servers)

	fmt.Println("==================================================")
	fmt.Println("       Raft Distributed KV Store Client           ")
	fmt.Println("==================================================")
	fmt.Printf("Connected cluster: %v\n", servers)
	printHelp()

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Printf("\nraft-kv [node:%d]> ", clerk.Leader())
		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		cmd := strings.ToLower(parts[0])

		switch cmd {
		case "exit", "quit":
			return
		case "help":
			printHelp()
		case "leader":
			fmt.Printf("Current suspected leader: Server %d (%s)\n", clerk.Leader(), servers[clerk.Leader()])
		case "get":
			if len(parts) < 2 {
				fmt.Println("usage: get <key>")
				continue
			}
			t0 := time.Now()
			val, ver, err := clerk.Get(parts[1])
			dur := time.Since(t0)

			if err == kvrpc.ErrNoKey {
				fmt.Printf("(nil) [key does not exist] (%v)\n", dur)
			} else if err == kvrpc.OK {
				fmt.Printf("value: %q  (version: %d) (%v)\n", val, ver, dur)
			} else {
				fmt.Printf("error: %s (%v)\n", err, dur)
			}

		case "put":
			if len(parts) < 3 {
				fmt.Println("usage: put <key> <val> [version]")
				continue
			}
			var version kvrpc.Tversion = 0
			if len(parts) >= 4 {
				fmt.Sscanf(parts[3], "%d", &version)
			}

			t0 := time.Now()
			err := clerk.Put(parts[1], parts[2], version)
			dur := time.Since(t0)

			if err == kvrpc.OK {
				fmt.Printf("OK (%v)\n", dur)
			} else {
				fmt.Printf("failed: %s (%v)\n", err, dur)
			}

		default:
			fmt.Printf("unknown command: %s. Type 'help' for options.\n", cmd)
		}
	}
}
