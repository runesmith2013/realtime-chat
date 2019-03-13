package dbclient

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/segmentio/kafka-go"
	"realtime-chat/model"
)

type KafkaClient struct {
	Messages []model.Message
	Topic    string
}

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

		kc.Messages = append(kc.Messages, message)

		if len(kc.Messages) > 10 {
			kc.Messages = kc.Messages[1:len(kc.Messages)]
		}

		fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))

		if err != nil {

			break
		}
	}

	r.Close()

}

func (kc *KafkaClient) GetMessages() ([]model.Message, error) {
	return kc.Messages, nil
}

func (kc *KafkaClient) AddMessage(message model.Message) error {

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   kc.Topic,
	})

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
