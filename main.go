package rmq

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/isayme/go-amqp-reconnect/rabbitmq"
	"github.com/streadway/amqp"
)

type RMQConfig struct {
	Host          string
	Login         string
	Password      string
	Exchange      string
	Queue         string
	Routes        map[string]func([]byte) ([]byte, error)
	PrefetchCount int
}

type RMQ interface {
	Connect(path RMQConfig)
	Send(topic string, message string) string
}

type RMQService struct {
	conn           *rabbitmq.Connection
	ch             *rabbitmq.Channel
	configs        RMQConfig
	replyQueue     amqp.Queue
	exitCh         chan bool
	correlationMap sync.Map
}

func NewRMQService() RMQService {
	return RMQService{}
}

func (s *RMQService) Connect(config RMQConfig) bool {
	rabbitmq.Debug = true
	s.configs = config
	s.exitCh = make(chan bool)
	connectionString := fmt.Sprintf("amqp://%s:%s@%s:5672", config.Login, config.Password, config.Host)
	conn, err := rabbitmq.Dial(connectionString)
	s.conn = conn
	failOnError(err, "Failed to connect to RabbitMQ")
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	s.ch = ch
	err = ch.Qos(
		config.PrefetchCount,
		0,
		false,
	)
	if s.configs.Exchange != "" {
		s.ch.ExchangeDeclare(s.configs.Exchange, "topic", true, false, false, false, make(amqp.Table))
	}
	failOnError(err, "Failed to set QoS")
	s.listenReply()

	if config.Queue != "" {
		ch.QueueDeclare(
			config.Queue,
			true,
			false,
			false,
			false,
			nil,
		)
		failOnError(err, "Failed to declare a queue")
		for path := range config.Routes {
			err = ch.QueueBind(config.Queue, path, config.Exchange, false, nil)
			failOnError(err, fmt.Sprintf("failed to bind queue %s", path))
		}
	}
	return true
}

func (s *RMQService) Disconnect() {
	s.ch.Close()
	s.conn.Close()
	s.exitCh <- true
	close(s.exitCh)
}

func (s *RMQService) Send(topic string, msg []byte, resp chan []byte) {
	correlationId := randomString(32)
	err := s.ch.Publish(
		s.configs.Exchange,
		topic,
		false,
		false,
		amqp.Publishing{
			ContentType:   "text/json",
			Body:          msg,
			CorrelationId: correlationId,
			ReplyTo:       s.replyQueue.Name,
		})
	failOnError(err, "Failed to publish a message")
	s.correlationMap.Store(correlationId, resp)
}

func (s *RMQService) Listen() {
	msgs, err := s.ch.Consume(
		s.configs.Queue,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to register a consumer")
	go func() {
		for msg := range msgs {
			resp, err := s.configs.Routes[msg.RoutingKey](msg.Body)
			headers := make(map[string]interface{})
			if err != nil {
				headers["-x-error"] = err.Error()
			} else {
				headers["done"] = "ok"
			}
			err = s.ch.Publish(
				"",
				msg.ReplyTo,
				false,
				false,
				amqp.Publishing{
					ContentType:   "text/json",
					CorrelationId: msg.CorrelationId,
					Body:          resp,
					Headers:       headers,
				})
			failOnError(err, "Failed to publish a message")
			msg.Ack(false)
		}
	}()
	log.Printf("[*] Awaiting RPC requests")
	<-s.exitCh
}

func (s *RMQService) listenReply() {
	q, err := s.ch.QueueDeclare(
		"",
		false,
		false,
		true,
		false,
		nil,
	)
	s.replyQueue = q
	failOnError(err, "Failed to declare replyQueue")
	msgs, err := s.ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to register a consumer on replyQueue")
	go func() {
		for msg := range msgs {
			replyChannelValue, ok := s.correlationMap.LoadAndDelete(msg.CorrelationId)
			if ok {
				replyChannel := replyChannelValue.(chan []byte)
				replyChannel <- msg.Body
			}
		}
	}()
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func randomString(l int) string {
	bytes := make([]byte, l)
	for i := 0; i < l; i++ {
		bytes[i] = byte(randInt(65, 90))
	}
	return string(bytes)
}

func randInt(min int, max int) int {
	rand.Seed(time.Now().UnixNano())
	return min + rand.Intn(max-min)
}

func DecodeMsg[T interface{}](body []byte) (T, error) {
	var msg T
	err := json.Unmarshal(body, &msg)
	if err != nil {
		return msg, err
	}
	return msg, nil
}

func EncodeMsg[T interface{}](resp T) ([]byte, error) {
	reply, err := json.Marshal(resp)
	if err != nil {
		return nil, err
	}
	return reply, nil
}
