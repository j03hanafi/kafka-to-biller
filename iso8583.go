package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/mofax/iso8583"
	"github.com/rivo/uniseg"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"time"
)

func sendIso(writer http.ResponseWriter, request *http.Request) {

	var response Response
	var iso Iso8583

	reqBody, _ := ioutil.ReadAll(request.Body)
	req := string(reqBody)
	log.Printf("ISO Message: %v\n", req)

	err := doProducer(broker, topic1, req)

	if err != nil {
		errDesc := fmt.Sprintf("Failed sent to Kafka\nError: %v", err)
		log.Println(err)
		response.ResponseCode, response.ResponseDescription = 500, errDesc
		jsonFormatter(writer, response, 500)
	} else {
		msg, err := consumeResponse(broker, group, []string{topic2})
		if err != nil {
			errDesc := fmt.Sprintf("Failed to get response from Kafka\nError: %v", err)
			log.Println(err)
			response.ResponseCode, response.ResponseDescription = 500, errDesc
			jsonFormatter(writer, response, 500)
		} else {

			if msg == "" {
				errDesc := "Got empty response"
				log.Println(errDesc)
				response.ResponseCode, response.ResponseDescription = 500, errDesc
				jsonFormatter(writer, response, 500)
			} else {

				header := msg[0:4]
				data := msg[4:]

				isoStruct := iso8583.NewISOStruct("spec1987.yml", false)

				isoParsed, err := isoStruct.Parse(data)
				if err != nil {
					log.Printf("Error parsing iso message\nError: %v", err)
				}

				iso.Header, _ = strconv.Atoi(header)
				iso.MTI = isoParsed.Mti.String()
				iso.Hex, _ = iso8583.BitMapArrayToHex(isoParsed.Bitmap)

				iso.Message, err = isoParsed.ToString()
				if err != nil {
					log.Printf("Iso Parsed failed convert to string.\nError: %v", err)
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
	data := message[4:]

	isoStruct := iso8583.NewISOStruct("spec1987.yml", false)

	msg, err := isoStruct.Parse(data)
	if err != nil {
		log.Println(err)
	}

	jsonIso := convertToJson(msg)
	serverResp := responseJson(jsonIso)
	isoParsed := convertIso(serverResp)

	isoParsed.AddMTI("0210")
	isoMessage, _ := isoParsed.ToString()
	isoMTI := isoParsed.Mti.String()
	isoHex, _ := iso8583.BitMapArrayToHex(isoParsed.Bitmap)
	isoHeader := fmt.Sprintf("%04d", uniseg.GraphemeClusterCount(isoMessage))

	response.Header, _ = strconv.Atoi(isoHeader)
	response.MTI = isoMTI
	response.Hex = isoHex
	response.Message = isoMessage

	event := isoHeader + isoMessage

	log.Printf("\n\nResponse: \n\tHeader: %v\n\tMTI: %v\n\tHex: %v\n\tIso Message: %v\n\tFull Message: %v\n\n",
		response.Header,
		response.MTI,
		response.Hex,
		response.Message,
		event)

	// create file from response
	filename := "Response_to_" + isoParsed.Elements.GetElements()[3] + "@" + fmt.Sprintf(time.Now().Format("2006-01-02 15:04:05"))
	file := CreateFile("storage/response/"+filename, event)
	log.Println("File created: ", file)

	err = doProducer(broker, topic2, event)
	if err != nil {
		log.Printf("Error producing message %v\n", message)
		log.Println(err)
	}

}

func responseJson(jsonIso Transaction) Transaction {
	var response PaymentResponse
	var transaction Transaction

	requestBody, err := json.Marshal(jsonIso)
	if err != nil {
		log.Fatalf("Preparing body request failed. Error: %v\n", err)
	}

	timeout := time.Duration(5 * time.Second)
	client := http.Client{
		Timeout: timeout,
	}

	log.Printf("Request to https://tiruan.herokuapp.com/biller\n")

	req, err := http.NewRequest("GET", "https://tiruan.herokuapp.com/biller", bytes.NewBuffer(requestBody))
	req.Header.Set("Content-Type", "application/json")
	if err != nil {
		log.Fatalf("Failed to sent request to https://tiruan.herokuapp.com/biller. Error: %v\n", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Fatalf("Failed to get response from https://tiruan.herokuapp.com/biller. Error: %v\n", err)
	}

	defer resp.Body.Close()

	log.Printf("Response from https://tiruan.herokuapp.com/biller\n")

	body, _ := ioutil.ReadAll(resp.Body)
	json.Unmarshal(body, &response)

	transaction = response.TransactionData

	return transaction
}

func convertIso(transaction Transaction) iso8583.IsoStruct {

	log.Println("Converting JSON to ISO8583")

	cardAcceptorTerminalId := transaction.CardAcceptorData.CardAcceptorTerminalId
	cardAcceptorName := transaction.CardAcceptorData.CardAcceptorName
	cardAcceptorCity := transaction.CardAcceptorData.CardAcceptorCity
	cardAcceptorCountryCode := transaction.CardAcceptorData.CardAcceptorCountryCode

	if len(transaction.CardAcceptorData.CardAcceptorTerminalId) < 16 {
		cardAcceptorTerminalId = rightPad(transaction.CardAcceptorData.CardAcceptorTerminalId, 16, " ")
	}
	if len(transaction.CardAcceptorData.CardAcceptorName) < 25 {
		cardAcceptorName = rightPad(transaction.CardAcceptorData.CardAcceptorName, 25, " ")
	}
	if len(transaction.CardAcceptorData.CardAcceptorCity) < 13 {
		cardAcceptorCity = rightPad(transaction.CardAcceptorData.CardAcceptorCity, 13, " ")
	}
	if len(transaction.CardAcceptorData.CardAcceptorCountryCode) < 2 {
		cardAcceptorCountryCode = rightPad(transaction.CardAcceptorData.CardAcceptorCountryCode, 2, " ")
	}
	cardAcceptor := cardAcceptorName + cardAcceptorCity + cardAcceptorCountryCode

	trans := map[int64]string{
		2:  transaction.Pan,
		3:  transaction.ProcessingCode,
		4:  strconv.Itoa(transaction.TotalAmount),
		5:  transaction.SettlementAmount,
		6:  transaction.CardholderBillingAmount,
		7:  transaction.TransmissionDateTime,
		9:  transaction.SettlementConversionRate,
		10: transaction.CardHolderBillingConvRate,
		11: transaction.Stan,
		12: transaction.LocalTransactionTime,
		13: transaction.LocalTransactionDate,
		17: transaction.CaptureDate,
		18: transaction.CategoryCode,
		22: transaction.PointOfServiceEntryMode,
		37: transaction.Refnum,
		41: cardAcceptorTerminalId,
		43: cardAcceptor,
		48: transaction.AdditionalData,
		49: transaction.Currency,
		50: transaction.SettlementCurrencyCode,
		51: transaction.CardHolderBillingCurrencyCode,
		57: transaction.AdditionalDataNational,
	}

	one := iso8583.NewISOStruct("spec1987.yml", false)
	spec, _ := specFromFile("spec1987.yml")

	if one.Mti.String() != "" {
		log.Printf("Empty generates invalid MTI")
	}

	for field, data := range trans {

		fieldSpec := spec.fields[int(field)]

		if fieldSpec.LenType == "fixed" {
			lengthValidate, _ := iso8583.FixedLengthIntegerValidator(int(field), fieldSpec.MaxLen, data)

			if lengthValidate == false {
				if fieldSpec.ContentType == "n" {
					data = leftPad(data, fieldSpec.MaxLen, "0")
				} else {
					data = rightPad(data, fieldSpec.MaxLen, " ")
				}
			}
		}

		one.AddField(field, data)

		log.Printf("Field[%s]: %s (%v)", strconv.Itoa(int(field)), data, fieldSpec.Label)

	}

	log.Println("Convert Success")
	return one
}
