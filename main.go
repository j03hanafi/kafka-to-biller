package main

import (
	"log"
	"net/http"
)

func main() {

	topics := []string{topic1, topic2}
	done := doConsume(broker, group, topics)

	router := server()
	serverErr := http.ListenAndServe(":6020", router)
	done <- true

	if serverErr != nil {
		log.Fatal(serverErr)
	}

}
