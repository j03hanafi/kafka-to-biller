package main

import (
	"fmt"
	"github.com/mofax/iso8583"
	"io/ioutil"
	"net/http"
	"strconv"
)

func sendIso(writer http.ResponseWriter, request *http.Request) {

	var response Response
	var iso Iso8583

	reqBody, _ := ioutil.ReadAll(request.Body)
	req := string(reqBody)
	fmt.Printf("ISO Message: %v\n", req)

	err := doProducer(broker, topic1, req)

	if err != nil {
		errDesc := fmt.Sprintf("Failed sent to Kafka\nError: %v", err)
		response.ResponseCode, response.ResponseDescription = 500, errDesc
		jsonFormatter(writer, response, 500)
	} else {
		msg, err := consumeResponse(broker, group, []string{topic2})
		if err != nil {
			errDesc := fmt.Sprintf("Failed to get response from Kafka\nError: %v", err)
			response.ResponseCode, response.ResponseDescription = 500, errDesc
			jsonFormatter(writer, response, 500)
		} else {

			if msg == "" {
				response.ResponseCode, response.ResponseDescription = 500, "Got empty response"
				jsonFormatter(writer, response, 500)
			} else {

				header := msg[0:4]
				data := msg[4:]

				isoStruct := iso8583.NewISOStruct("spec1987.yml", false)

				isoParsed, err := isoStruct.Parse(data)
				if err != nil {
					fmt.Printf("Error parsing iso message\nError: %v", err)
				}

				iso.Header, _ = strconv.Atoi(header)
				iso.MTI = isoParsed.Mti.String()
				iso.Hex, _ = iso8583.BitMapArrayToHex(isoParsed.Bitmap)

				iso.Message, err = isoParsed.ToString()
				if err != nil {
					fmt.Printf("Iso Parsed failed convert to string.\nError: %v", err)
				}

				//event := header + iso.Message

				iso.ResponseStatus.ResponseCode, iso.ResponseStatus.ResponseDescription = 200, "Success"
				jsonFormatter(writer, iso, 200)

			}

		}
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

	err = doProducer(broker, topic2, event)
	if err != nil {
		fmt.Errorf("Error producing message %v\n", message)
	}

}
