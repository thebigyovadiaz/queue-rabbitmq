package main

import (
	"encoding/json"
	gopherrabbit "github.com/masnun/gopher-and-rabbit"
	"github.com/streadway/amqp"
	"log"
	"os"
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

	err = amqpChannel.Qos(1, 0, false)
	HandleError(err, "Couldn't config Qos")

	messageChannel, err := amqpChannel.Consume(queue.Name, "", false, false, false, false, nil)
	HandleError(err, "Cannot register consumer")

	stopChannel := make(chan bool)

	go func() {
		log.Printf("Our consumer ready, PID: %d", os.Getpid())
		for d := range messageChannel {
			log.Printf("Received message: %s", d.Body)

			addTask := &gopherrabbit.AddTask{}

			err := json.Unmarshal(d.Body, addTask)
			if err != nil {
				log.Printf("Error in decoding JSON %s", err)
			}
			log.Printf("Result of %d + %d is: %d", addTask.Number1, addTask.Number2, addTask.Number1+addTask.Number2)

			if err := d.Ack(false); err != nil {
				log.Printf("Error message: %s", err)
			} else {
				log.Printf("Ack message")
			}
		}
	}()

	<-stopChannel
}
