package main

import (
	"log"
	"net/http"
)

func main() {

	done := doConsume(broker, group)

	router := server()
	serverErr := http.ListenAndServe(":6020", router)
	done <- true

	if serverErr != nil {
		log.Fatal(serverErr)
	}

}
