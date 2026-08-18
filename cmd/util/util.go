package util

import "fmt"

func DynamicPorts(numPeers int) []string {
	ports := []string{}
	start := 8000
	end := start + numPeers

	for port := start; port < end; port++ {
		addr := fmt.Sprintf("127.0.0.1:%d", port)
		ports = append(ports, addr)
	}

	return ports
}
