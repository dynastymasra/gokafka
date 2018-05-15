package main

import (
	"fmt"
	"os"
	"reflect"
	"runtime"

	"os/signal"
	"syscall"

	"github.com/Shopify/sarama"
	log "github.com/dynastymasra/gochill"
	"github.com/satori/go.uuid"
)

func init() {
	os.Setenv("SERVICE_NAME", "Gokafka")
}

func main() {
	log.Info(log.Msg("Run kafka producer with samara library"),
		log.O("package", runtime.FuncForPC(reflect.ValueOf(main).Pointer()).Name()))

	stop := make(chan os.Signal)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true

	brokers := []string{"localhost:9092"}
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Alert(log.Msg("Failed create sync producer", err.Error()),
			log.O("package", runtime.FuncForPC(reflect.ValueOf(main).Pointer()).Name()),
			log.O("brokers", brokers))
		os.Exit(1)
	}
	defer producer.Close()

	message := &sarama.ProducerMessage{
		Topic: "test",
		Value: sarama.StringEncoder(fmt.Sprintf("UUID > %v", uuid.NewV4().String())),
	}

	partition, offset, err := producer.SendMessage(message)
	if err != nil {
		log.Alert(log.Msg("Failed send single message to broker", err.Error()),
			log.O("package", runtime.FuncForPC(reflect.ValueOf(main).Pointer()).Name()),
			log.O("brokers", brokers), log.O("topic", message.Topic))
		os.Exit(1)
	}

	log.Info(log.Msg("Message success send single message to broker"), log.O("partition", partition),
		log.O("package", runtime.FuncForPC(reflect.ValueOf(main).Pointer()).Name()),
		log.O("brokers", brokers), log.O("topic", message.Topic), log.O("offset", offset))

	var messages []*sarama.ProducerMessage
	for i := 0; i < 10; i++ {
		message := &sarama.ProducerMessage{
			Topic: "test",
			Value: sarama.StringEncoder(fmt.Sprintf("UUID > %v", uuid.NewV4().String())),
		}
		messages = append(messages, message)
	}

	if err := producer.SendMessages(messages); err != nil {
		log.Alert(log.Msg("Failed send multiple message to broker", err.Error()),
			log.O("package", runtime.FuncForPC(reflect.ValueOf(main).Pointer()).Name()),
			log.O("brokers", brokers), log.O("topic", message.Topic))
		os.Exit(1)
	}

	log.Info(log.Msg("Message success send multiple message to broker"), log.O("partition", partition),
		log.O("package", runtime.FuncForPC(reflect.ValueOf(main).Pointer()).Name()),
		log.O("brokers", brokers))

	go func(producer sarama.SyncProducer) {
		for {
			message := &sarama.ProducerMessage{
				Topic: "test",
				Value: sarama.StringEncoder(fmt.Sprintf("UUID > %v", uuid.NewV4().String())),
			}

			partition, offset, err := producer.SendMessage(message)
			if err != nil {
				log.Alert(log.Msg("Failed send multiple message to broker", err.Error()),
					log.O("package", runtime.FuncForPC(reflect.ValueOf(main).Pointer()).Name()),
					log.O("partition", partition), log.O("offset", offset))
				os.Exit(1)
			}
		}
	}(producer)

	log.Warn(log.Msg("Run infinite loop send single message", "This must stop manually!"),
		log.O("package", runtime.FuncForPC(reflect.ValueOf(main).Pointer()).Name()))

	select {
	case sig := <-stop:
		log.Info(log.Msg("Application prepare to shutdown", fmt.Sprintf("%+v", sig)),
			log.O("package", runtime.FuncForPC(reflect.ValueOf(main).Pointer()).Name()))
		os.Exit(0)
	}
}
