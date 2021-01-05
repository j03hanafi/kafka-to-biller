package main

import (
	_ "fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

func CreateFile(fileName string, content string) string {

	if !strings.Contains(fileName, ".txt") {
		fileName += ".txt"
	}

	file, err := os.Create(fileName)

	if err != nil {
		log.Fatalf("failed creating file: %s", err)
	}

	defer file.Close()

	_, err = file.WriteString(content)

	if err != nil {
		log.Fatalf("failed writing to file: %s", err)
	}

	return fileName

}

func CheckExist(namaFile string) bool {

	info, err := os.Stat(namaFile)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func ReadFile(fileName string) string {

	data, err := ioutil.ReadFile(fileName)
	if err != nil {
		log.Panicf("failed reading data from file: %s", err)
	}

	return string(data)

}
