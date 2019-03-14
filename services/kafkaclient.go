package services

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/rs/xid"
	"github.com/segmentio/kafka-go"
	"realtime-chat/model"
)

type KafkaClient struct {
	Messages []model.Message
	Topic    string
}

var locationId = xid.New().String()

func (kc *KafkaClient) ConnectToTopic() {
	kc.Messages = make([]model.Message, 0)

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"localhost:9092"},
		Topic:     kc.Topic,
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})

	r.SetOffset(0)

	for {
		m, err := r.ReadMessage(context.Background())

		message := model.Message{}
		json.Unmarshal(m.Value, &message)

		if message.SourceLocation != locationId {
			Manager.Broadcast <- m.Value
		}

		fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))

		if err != nil {
			break
		}
	}

	r.Close()

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
