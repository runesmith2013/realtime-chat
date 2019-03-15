package services

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/rs/xid"
	"github.com/segmentio/kafka-go"
	"realtime-chat/model"
	"time"
)

type KafkaClient struct {
	Topic    string
}

var locationId = xid.New().String()

func (kc *KafkaClient) ConnectToTopic() {

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"localhost:9092"},
		Topic:     kc.Topic,
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})

	r.SetOffset(kafka.LastOffset);

	for {
		m, err := r.ReadMessage(context.Background())

		if err != nil {
			fmt.Printf("Error reading message from Kafka queue");
		}

		message := model.Message{}
		err = json.Unmarshal(m.Value, &message)

		if err != nil {
			fmt.Printf("Unable to parse message %s = %s\n", string(m.Key), string(m.Value))
		}

		if message.SourceLocation != locationId {
			Manager.Broadcast <- m.Value
		} else {
			fmt.Printf("Ignoring message from source location. \n" )

		}

		fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))

		if err != nil {
			break
		}
	}

	r.Close()

}


func (kc *KafkaClient) Connect() {
	// to consume messages
	topic := "test0"
	partition := 0

	conn, _ := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)

	conn.SetReadDeadline(time.Now().Add(10*time.Second))
	batch := conn.ReadBatch(10e3, 1e6) // fetch 10KB min, 1MB max

	b := make([]byte, 10e3) // 10KB max per message
	for {
		_, err := batch.Read(b)
		if err != nil {
			break
		}
		fmt.Println(string(b))
	}

	batch.Close()
	conn.Close()
}

func (kc *KafkaClient) SendMessage(message model.Message) error {

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   kc.Topic,
	})

	message.Id = xid.New().String()
	message.SourceLocation = locationId

	// Serialize the struct to JSON
	jsonBytes, _ := json.Marshal(message)

	err := w.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(message.Id),
			Value: []byte(jsonBytes),
		},
	)
	return err

}
