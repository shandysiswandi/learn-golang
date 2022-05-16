package main

import (
	"context"
	"flag"
	"fmt"

	"github.com/segmentio/kafka-go"
)

const (
	groupID       = "learn-golang-group"
	key           = "learn-golang-key"
	topic         = "learn-golang"
	brokerAddress = "localhost:9092"
	partition     = 0
)

func init() {
	conn, err := kafka.DialLeader(context.Background(), "tcp", brokerAddress, topic, 0)
	if err != nil {
		panic(err.Error())
	}
	fmt.Println("OK")
	conn.Close()
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
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{brokerAddress},
		Topic:   topic,
	})

	if msg == "" || msg == "default" {
		msg = `{"key":"value"}`
	}

	err := w.WriteMessages(ctx, kafka.Message{Key: []byte(key), Value: []byte(msg)})
	if err != nil {
		fmt.Println("producer:", err)
		return
	}
	fmt.Println("producer: successfully publish message")
}

func runConsumer(ctx context.Context) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddress},
		Topic:   topic,
		GroupID: groupID,
	})

	for {
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			fmt.Println("consumer:", err)
			return
		}

		fmt.Println("consumer: successfully subscribe message", string(msg.Value))
	}
}
