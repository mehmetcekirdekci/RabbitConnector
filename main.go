package main

import (
	"context"
	"encoding/json"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"time"
)

type GolangRabbitConnectorMessage struct {
	Name     string
	LastName string
	Age      int
	Gender   string
}

func main() {

	message := GolangRabbitConnectorMessage{
		Name:     "Mehmet",
		LastName: "Çekirdekci",
		Age:      25,
		Gender:   "Male",
	}

	queueName := "GolangRabbitConnectorMessageQueue"

	publish(message, queueName)
}

func publish(message GolangRabbitConnectorMessage, queueName string) {
	send(message, queueName)
}

func getConnection() (*amqp.Channel, error) {
	var connection *amqp.Connection
	var ch *amqp.Channel
	var err error

	connection, err = amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		fmt.Printf("Can not connected to rabbit. Error: %s", err.Error())
	}

	ch, err = connection.Channel()
	if err != nil {
		fmt.Printf("Can not open the channel. Error: %s", err.Error())
	}

	return ch, nil
}

func send(message GolangRabbitConnectorMessage, queueName string) {
	var ch *amqp.Channel
	var queue amqp.Queue
	var messageBody []byte
	var err error

	ch, err = getConnection()
	if err != nil {
		fmt.Printf("There is an error. Error: %s", err.Error())
	}

	queue, err = ch.QueueDeclare(
		queueName, // name
		false,     // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)

	if err != nil {
		fmt.Printf("Queue can not be declared. Error: %s", err.Error())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	messageBody, err = json.Marshal(message)
	if err != nil {
		fmt.Printf("Message can not be parsed. Error: %s", err.Error())
	}

	err = ch.PublishWithContext(ctx,
		"",         // exchange
		queue.Name, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        messageBody,
		})

	ch.Close()
}

func consume() {

}
