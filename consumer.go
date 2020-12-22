package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"os"
	"os/signal"
	"syscall"
)

func doConsume(broker string, group string, topics []string) (err error) {

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	cm := kafka.ConfigMap{
		"bootstrap.servers":    broker,
		"group.id":             group,
		"auto.offset.reset":    "earliest",
		"enable.partition.eof": true,
	}

	c, err := kafka.NewConsumer(&cm)

	// check if there's error in creating The Consumer
	if err != nil {
		if ke, ok := err.(kafka.Error); ok == true {
			switch ec := ke.Code(); ec {
			case kafka.ErrInvalidArg:
				return fmt.Errorf("Can't create consumer because wrong configuration (code: %d)!\n\t%v\n\nTo see the configuration options, refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md\n", ec, err)
			default:
				return fmt.Errorf("Can't create consumer (code: %d)!\n\t%v\n", ec, err)
			}
		} else {
			// Not Kafka Error occurs
			return fmt.Errorf("Can't create consumer because generic error! \n\t%v\n", err)
		}
	} else {

		// subscribe to the topic
		if err := c.SubscribeTopics(topics, nil); err != nil {
			return fmt.Errorf("There's an Error subscribing to the topic:\n\t%v\n", err)
		}

		doTerm := false

		for !doTerm {
			select {
			case sig := <-sigchan:
				fmt.Printf("Caught signal %v: terminating\n", sig)
				doTerm = true
			default:
				ev := c.Poll(1000)
				if ev == nil {
					continue
				}

				switch ev.(type) {

				case *kafka.Message:
					// It's a message
					km := ev.(*kafka.Message)
					fmt.Printf("Message '%v' received from topic '%v' (partition %d at offset %d)\n",
						string(km.Value),
						*km.TopicPartition.Topic,
						km.TopicPartition.Partition,
						km.TopicPartition.Offset)
					if km.Headers != nil {
						fmt.Printf("Headers: %v\n", km.Headers)
					}

				case kafka.PartitionEOF:
					pe := ev.(kafka.PartitionEOF)
					fmt.Printf("Got to the end of partition %v on topic %v at offset %v\n",
						pe.Partition,
						*pe.Topic,
						pe.Offset)

				case kafka.OffsetsCommitted:
					continue

				case kafka.Error:
					// It's an error
					em := ev.(kafka.Error)
					return fmt.Errorf("☠️ Uh oh, caught an error:\n\t%v\n", em)

				default:
					// It's not anything we were expecting
					fmt.Printf("Got an event that's not a Message, Error, or PartitionEOF 👻\n\t%v\n", ev)

				}
			}
		}
		c.Close()
	}
	return nil
}
