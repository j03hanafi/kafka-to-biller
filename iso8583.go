package main

import (
	"fmt"
	"github.com/mofax/iso8583"
	"io/ioutil"
	"net/http"
	"strconv"
)

func sendIso(writer http.ResponseWriter, request *http.Request) {

	reqBody, _ := ioutil.ReadAll(request.Body)
	req := string(reqBody)
	fmt.Printf("ISO Message: %v", req)

	var response Iso8583
	header := req[0:4]
	data := req[4:]

	isoStruct := iso8583.NewISOStruct("spec1987.yml", false)

	msg, err := isoStruct.Parse(data)
	if err != nil {
		fmt.Println(err)
	}

	response.Header, _ = strconv.Atoi(header)
	response.MTI = msg.Mti.String()
	response.Hex, _ = iso8583.BitMapArrayToHex(msg.Bitmap)

	response.Message, err = msg.ToString()
	if err != nil {
		fmt.Println(err)
	}

	_, err = doProducer(broker, topic1, response.Message)
	if err != nil {
		response.ResponseStatus.ResponseCode, response.ResponseStatus.ResponseDescription = 500, "Failed sent to Kafka"
		jsonFormatter(writer, response, 500)
	} else {
		response.ResponseStatus.ResponseCode, response.ResponseStatus.ResponseDescription = 200, "Success"
		jsonFormatter(writer, response, 200)
	}

}

func responseIso(message string) {
	var response Iso8583
	header := message[0:4]
	data := message[4:]

	isoStruct := iso8583.NewISOStruct("spec1987.yml", false)

	msg, err := isoStruct.Parse(data)
	if err != nil {
		fmt.Println(err)
	}

	msg.AddMTI("0210")
	response.Header, _ = strconv.Atoi(header)
	response.MTI = msg.Mti.String()
	response.Hex, _ = iso8583.BitMapArrayToHex(msg.Bitmap)

	response.Message, err = msg.ToString()
	if err != nil {
		fmt.Println(err)
	}

	event := header + response.Message

	fmt.Printf("\n\nResponse: \n\tHeader: %v\n\tMTI: %v\n\tHex: %v\n\tIso Message: %v\n\tFull Message: %v\n\n",
		response.Header,
		response.MTI,
		response.Hex,
		response.Message,
		event)

	_, err = doProducer(broker, topic2, event)
	if err != nil {
		response.ResponseStatus.ResponseCode, response.ResponseStatus.ResponseDescription = 500, "Failed sent to Kafka"
	} else {
		response.ResponseStatus.ResponseCode, response.ResponseStatus.ResponseDescription = 200, "Success"
	}

}
