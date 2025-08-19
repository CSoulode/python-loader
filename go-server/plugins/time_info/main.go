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
	prod                  *rmq.Producer
	timeOfDayTagset       string = "Time of day (string)"
	dayOfWeekStringTagset string = "Day of week (string)"
	dayOfWeekNumberTagset string = "Day of week (number)"
	dayWithinMonthTagset  string = "Day within month"
	dayWithinYearTagset   string = "Day within year"
	monthNumberTagset     string = "Month (number)"
	monthStringTagset     string = "Month (string)"
	yearTagset            string = "Year (number)"
	yearMonthTagset       string = "Year Month"
	timeTagset            string = "Time"
	dateTagset            string = "Date"
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

	mediaId, ok := message["mediaID"]
	if !ok {
		log.Printf("Missing mediaID in message body")
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

	log.Printf("Message received : mediaID: %s, timestamp: %s", mediaId, timestamp)

	// Determine the time of day
	hour := timestamp.Hour()
	var timeOfDay string
	switch {
	case hour >= 5 && hour < 12:
		timeOfDay = "Morning"
	case hour >= 12 && hour < 17:
		timeOfDay = "Afternoon"
	case hour >= 17 && hour < 21:
		timeOfDay = "Evening"
	default:
		timeOfDay = "Night"
	}

	// Publish the result
	body := fmt.Sprintf(`{"taggingValue": "%s", "mediaID": "%s"}`, timeOfDay, mediaId)
	rmq.PublishMessage(prod, body, fmt.Sprintf("tagging.not_added.1.%s", timeOfDayTagset))

	dayOfWeek := timestamp.Weekday().String()

	body = fmt.Sprintf(`{"taggingValue": "%s", "mediaID": "%s"}`, dayOfWeek, mediaId)
	rmq.PublishMessage(prod, body, fmt.Sprintf("tagging.not_added.1.%s", dayOfWeekStringTagset))

	dayOfWeekNumber := timestamp.Weekday()
	body = fmt.Sprintf(`{"taggingValue": "%d", "mediaID": "%s"}`, int(dayOfWeekNumber), mediaId)
	rmq.PublishMessage(prod, body, fmt.Sprintf("tagging.not_added.5.%s", dayOfWeekNumberTagset))

	dayWithinMonth := timestamp.Day()
	body = fmt.Sprintf(`{"tagset": "%s", "taggingValue": "%d", "mediaID": "%s"}`, dayWithinMonthTagset, dayWithinMonth, mediaId)
	rmq.PublishMessage(prod, body, fmt.Sprintf("tagging.not_added.5.%s", dayWithinMonthTagset))

	dayWithinYear := timestamp.YearDay()
	body = fmt.Sprintf(`{"taggingValue": "%d", "mediaID": "%s"}`, dayWithinYear, mediaId)
	rmq.PublishMessage(prod, body, fmt.Sprintf("tagging.not_added.5.%s", dayWithinYearTagset))

	month := timestamp.Month()
	body = fmt.Sprintf(`{"taggingValue": "%d", "mediaID": "%s"}`, month, mediaId)
	rmq.PublishMessage(prod, body, fmt.Sprintf("tagging.not_added.5.%s", monthNumberTagset))

	monthString := month.String()
	body = fmt.Sprintf(`{"taggingValue": "%s", "mediaID": "%s"}`, monthString, mediaId)
	rmq.PublishMessage(prod, body, fmt.Sprintf("tagging.not_added.1.%s", monthStringTagset))

	year := timestamp.Year()
	body = fmt.Sprintf(`{"taggingValue": "%d", "mediaID": "%s"}`, year, mediaId)
	rmq.PublishMessage(prod, body, fmt.Sprintf("tagging.not_added.5.%s", yearTagset))

	yearMonth := fmt.Sprintf("%d-%d", year, month)
	body = fmt.Sprintf(`{"taggingValue": "%s", "mediaID": "%s"}`, yearMonth, mediaId)
	rmq.PublishMessage(prod, body, fmt.Sprintf("tagging.not_added.1.%s", yearMonthTagset))

	timeString := timestamp.Format("15:04:05")
	body = fmt.Sprintf(`{"taggingValue": "%s", "mediaID": "%s"}`, timeString, mediaId)
	rmq.PublishMessage(prod, body, fmt.Sprintf("tagging.not_added.3.%s", timeTagset))

	dateString := timestamp.Format("2006-01-02")
	body = fmt.Sprintf(`{"taggingValue": "%s", "mediaID": "%s"}`, dateString, mediaId)
	rmq.PublishMessage(prod, body, fmt.Sprintf("tagging.not_added.4.%s", dateTagset))

}
