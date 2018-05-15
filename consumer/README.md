# GoKafka

[![Go](https://img.shields.io/badge/go-1.10.1-00E5E6.svg)](https://golang.org/)
[![Kafka](https://img.shields.io/badge/kafka-0.10.1-000000.svg)](https://kafka.apache.org/)
[![Glide](https://img.shields.io/badge/glide-0.12.3-CFBDB1.svg)](https://glide.sh/)

This sample Golang use kafka for message broker. This used for consume message from kafka broker.

Consumer use [Job/Worker pattern](http://marcio.io/2015/07/handling-1-million-requests-per-minute-with-golang/)

## Libraries
Use glide command for install all dependencies required this application.
  - Use command `glide install` for install all dependency.
  - Use command `glide up` for update all dependency.
  
## How To Run and Deploy

Use command go `go run *.go` in root folder for run this application.