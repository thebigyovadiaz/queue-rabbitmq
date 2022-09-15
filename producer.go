package main

import (
	"encoding/json"
	gopherrabbit "github.com/masnun/gopher-and-rabbit"
	"github.com/streadway/amqp"
	"log"
	"math/rand"
	"time"
)

func HandleError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	conn, err := amqp.Dial(gopherrabbit.Config.AMQPConnectionURL)
	HandleError(err, "Cannot connected")
	defer conn.Close()

	amqpChannel, err := conn.Channel()
	HandleError(err, "Cannot created AMQP Channel")
	defer amqpChannel.Close()

	queue, err := amqpChannel.QueueDeclare("add", true, false, false, false, nil)
	HandleError(err, "Couldn't declare add queue")

	rand.Seed(time.Now().UnixNano())

	addTask := gopherrabbit.AddTask{Number1: rand.Intn(999), Number2: rand.Intn(999)}
	body, err := json.Marshal(addTask)
	if err != nil {
		HandleError(err, "Error encoding JSON")
	}

	err = amqpChannel.Publish("", queue.Name, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "text/plain",
		Body:         body,
	})

	if err != nil {
		log.Fatalf("Error publishing the message %s", err)
	}

	log.Printf("AddTask: %d+%d", addTask.Number1, addTask.Number2)
}
