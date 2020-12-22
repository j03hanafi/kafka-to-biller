package main

import (
	"fmt"
	"os"
)

func main() {

	if len(os.Args) < 4 {
		fmt.Fprintf(os.Stderr, "Usage: %s <broker> <group> <topics..>\n",
			os.Args[0])
		os.Exit(1)
	}

	broker := os.Args[1]
	group := os.Args[2]
	topics := os.Args[3:]

	err := doConsume(broker, group, topics)
	if err != nil {
		os.Exit(1)
	}

	//	nethttp
	//  httplistenandserve
}
