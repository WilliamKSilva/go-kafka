package main

import (
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	producer := NewKafkaProducer()
	Publish("mensagem", "teste", producer, nil)
	producer.Flush(1000)
}

func NewKafkaProducer() *kafka.Producer {
	configmap := &kafka.ConfigMap{
		"bootstrap.servers": "go-kafka_kafka_1:9092",
	}

	p, err := kafka.NewProducer(configmap)
	if err != nil {
		log.Println(err)
	}

	return p
}

func Publish(message string, topic string, producer *kafka.Producer, key []byte) error {
	msg := &kafka.Message{
		Value:          []byte(message),
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            key,
	}

	err := producer.Produce(msg, nil)

	if err != nil {
		return err
	}

	return nil
}
