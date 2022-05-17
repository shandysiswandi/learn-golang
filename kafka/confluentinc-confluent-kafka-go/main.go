package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
	topic = "learn-golang"
)

func init() {
	a, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
	if err != nil {
		fmt.Printf("Failed to create Admin client: %s\n", err)
		os.Exit(1)
	}
	defer a.Close()

	_, err = a.CreateTopics(context.Background(),
		[]kafka.TopicSpecification{{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 0}},
	)
	if err != nil {
		fmt.Println("Init ", err)
		return
	}
}

func main() {
	producerFlag := flag.Bool("producer", false, "go run kafka/segmention-kafka/main.go -producer")
	producerMsgFlag := flag.String("message", "default", "go run kafka/segmention-kafka/main.go -message")
	consumerFlag := flag.Bool("consumer", false, "go run kafka/segmention-kafka/main.go -consumer")
	flag.Parse()
	ctx := context.Background()

	if *producerFlag && *consumerFlag {
		fmt.Println("=====================================================")
		fmt.Println("Can't run producer and consumer with one line command")
		fmt.Println("=====================================================")
		return
	}

	if !*producerFlag && !*consumerFlag {
		fmt.Println("======================================================================")
		fmt.Println("You have to run producer and consumer with -producer or -consumer flag")
		fmt.Println("======================================================================")
		return
	}

	if *producerFlag {
		runProducer(ctx, *producerMsgFlag)
		return
	}

	if *consumerFlag {
		runConsumer(ctx)
		return
	}
}

func runProducer(ctx context.Context, msg string) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
	if err != nil {
		fmt.Println("producer:", err)
		return
	}
	defer p.Close()

	if msg == "" || msg == "default" {
		msg = `{"key":"value"}`
	}

	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(msg),
	}, nil)
	if err != nil {
		fmt.Println("producer:", err)
		return
	}

	p.Flush(5000 /*5 seconds*/)
}

func runConsumer(ctx context.Context) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"group.id":          "myGroup",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		fmt.Println("producer:", err)
		return
	}
	defer c.Close()

	c.SubscribeTopics([]string{topic}, nil)
	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		} else {
			// The client will automatically try to recover from all errors.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}
}
