package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	rmq "m3.dataloader/rabbitMQ"
)

var (
	prod *rmq.Producer
)

func main() {
	prod = rmq.ProducerConnexionInit()
	defer prod.ConnexionEnd()
	log.Println("producer created")

	// Listen only to taggings of the type 2 (timestamp) (ignoring if they are already added to the database or not)
	rmq.Listen("tagging.*.2.*", processMessage)
}

func processMessage(d amqp.Delivery) {
	log.Println("new message")
	var message map[string]string
	err := json.Unmarshal(d.Body, &message)
	if err != nil {
		log.Printf("Failed to parse message body: %v", err)
		return
	}

	tag, ok := message["taggingValue"]
	if !ok {
		log.Printf("Missing taggingValue in message body")
		return
	}

	timestamp, err := time.Parse("2006-01-02 15:04:05", tag)
	if err != nil {
		log.Printf("failed to parse timestamp: %v", err)
		return
	}

	year := timestamp.Year()
	month := timestamp.Month()
	yearMonth := fmt.Sprintf("%d-%d", year, month)
	dateString := timestamp.Format("2006-01-02")

	hierarchy := fmt.Sprintf(`{
	"hierarchy": "Dates",
	"tagset": "Dates",
	"tagTypeId": 1,
	"tag": "Dates",
	"child": {
		"tagTypeId": %d,
		"tag": "%d",
		"tagset": "Year",
		"child": {
			"tag": "%s",
			"tagTypeId": 1,
			"tagset": "Year Month",
			"child": {
				"tag": "%s",
				"tagTypeId": 4,
				"tagset": "Date",
				"child": {}
				}
			}
		}
	}`, 5, year, yearMonth, dateString)

	// Publish the result
	rmq.PublishMessage(prod, hierarchy, "hierarchy")
}
