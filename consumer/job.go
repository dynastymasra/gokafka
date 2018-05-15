package main

import (
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
)

var JobQueue chan Job

type Job struct {
	Payload      *sarama.ConsumerMessage
	Notification *cluster.Notification
	Broker       string
	Topics       []string
	Error        error
}
