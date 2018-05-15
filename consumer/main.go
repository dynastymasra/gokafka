package main

import (
	"fmt"

	"os"
	"os/signal"
	"reflect"
	"runtime"
	"syscall"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	log "github.com/dynastymasra/gochill"
)

func init() {
	os.Setenv("SERVICE_NAME", "Gokafka")
}

func main() {
	log.Info(log.Msg("Prepare run kafka consumer service with samara"),
		log.O("package", runtime.FuncForPC(reflect.ValueOf(main).Pointer()).Name()))

	stop := make(chan os.Signal)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)

	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Group.Return.Notifications = true

	brokers := []string{"localhost:9092"}
	topics := []string{"test"}
	consumer, err := cluster.NewConsumer(brokers, "group_id", topics, config)
	if err != nil {
		log.Alert(log.Msg("Failed create consumer to broker", err.Error()),
			log.O("package", runtime.FuncForPC(reflect.ValueOf(main).Pointer()).Name()),
			log.O("brokers", brokers), log.O("topics", topics))
		os.Exit(1)
	}
	defer consumer.Close()

	log.Info(log.Msg("Application start running"), log.O("brokers", brokers),
		log.O("package", runtime.FuncForPC(reflect.ValueOf(main).Pointer()).Name()),
		log.O("topics", topics))

	JobQueue = make(chan Job, 5)
	go func() {
		dispatcher := NewDispatcher(5, JobQueue)
		dispatcher.Run()
	}()

	for {
		select {
		case notification := <-consumer.Notifications():
			log.Warn(log.Msg("Consumer notification", fmt.Sprintf("%+v", notification)),
				log.O("package", runtime.FuncForPC(reflect.ValueOf(main).Pointer()).Name()),
				log.O("brokers", brokers), log.O("topics", topics))
			JobQueue <- Job{Notification: notification, Error: nil}
		case err := <-consumer.Errors():
			log.Error(log.Msg("Consumer get errors", err.Error()),
				log.O("package", runtime.FuncForPC(reflect.ValueOf(main).Pointer()).Name()),
				log.O("brokers", brokers), log.O("topics", topics))
			JobQueue <- Job{Notification: nil, Error: err}
		case message := <-consumer.Messages():
			log.Info(log.Msg("Got message from broker", string(message.Value)), log.O("offset", message.Offset),
				log.O("package", runtime.FuncForPC(reflect.ValueOf(main).Pointer()).Name()),
				log.O("brokers", brokers), log.O("topic", message.Topic), log.O("partition", message.Partition))
			JobQueue <- Job{Notification: nil, Error: nil, Payload: message}
		case sig := <-stop:
			log.Info(log.Msg("Application prepare to shutdown", fmt.Sprintf("%+v", sig)),
				log.O("package", runtime.FuncForPC(reflect.ValueOf(main).Pointer()).Name()))
			close(JobQueue)
			os.Exit(0)
		}
	}
}
