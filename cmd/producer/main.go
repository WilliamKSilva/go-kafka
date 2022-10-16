package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	deliveryChan := make(chan kafka.Event)
	producer := NewKafkaProducer()
	Publish("transferência", "teste", producer, nil, deliveryChan)
	go DeliveryReport(deliveryChan)

	producer.Flush(2000)

	// e := <-deliveryChan
	// msg := e.(*kafka.Message)

	// if msg.TopicPartition.Error != nil {
	// 	fmt.Println("Erro ao enviar!")
	// } else {
	// 	fmt.Println("Mensagem enviada:", msg.TopicPartition)
	// }
}

func NewKafkaProducer() *kafka.Producer {
	configmap := &kafka.ConfigMap{
		"bootstrap.servers":   "go-kafka_kafka_1:9092",
		"delivery.timeout.ms": "0",
		"acks":                "all",
		"enable.idempotence":  "true",
	}

	p, err := kafka.NewProducer(configmap)
	if err != nil {
		log.Println(err)
	}

	return p
}

func Publish(message string, topic string, producer *kafka.Producer, key []byte, deliveryChan chan kafka.Event) error {
	msg := &kafka.Message{
		Value:          []byte(message),
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            key,
	}

	err := producer.Produce(msg, deliveryChan)

	if err != nil {
		return err
	}

	return nil
}

func DeliveryReport(deliveryChan chan kafka.Event) {
	for e := range deliveryChan {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				fmt.Println("Erro ao enviar")
			} else {
				fmt.Println("Mensagem enviada:", ev.TopicPartition)
				// anotar no banco de dados que a mensagem foi processado.
				// ex: confirma que uma transferencia bancaria ocorreu.
			}
		}
	}
}
