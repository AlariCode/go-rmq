package rmq

import (
	"fmt"
	"github.com/CHH/eventemitter"
	"github.com/isayme/go-amqp-reconnect/rabbitmq"
	"github.com/streadway/amqp"
	"log"
	"math/rand"
	"time"
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
	ch         *rabbitmq.Channel
	configs    RMQConfig
	replyQueue string
	replyEvent eventemitter.EventEmitter
}

func NewRMQService() RMQService {
	return RMQService{}
}

func (s *RMQService) Connect(config RMQConfig) {
	rabbitmq.Debug = true
	s.configs = config
	s.replyEvent = *eventemitter.New()
	s.replyQueue = "amq.rabbitmq.reply-to"
	connectionString := fmt.Sprintf("amqp://%s:%s@%s:5672", config.Login, config.Password, config.Host)
	conn, err := rabbitmq.Dial(connectionString)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	s.ch = ch
	defer ch.Close()
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
		err = ch.Qos(
			config.PrefetchCount,
			0,
			false,
		)
		failOnError(err, "Failed to set QoS")
		s.listen()
	}
}

func (s *RMQService) Send(topic string, msg []byte, resp chan []byte) {
	correlationId := randomString(12)
	err := s.ch.Publish(
		s.configs.Exchange,
		topic,
		false,
		false,
		amqp.Publishing{
			ContentType:   "text/json",
			Body:          msg,
			CorrelationId: correlationId,
		})
	failOnError(err, "Failed to publish a message")
	s.replyEvent.On(correlationId, func(msg []byte) {
		s.replyEvent.RemoveListeners(correlationId)
		resp <- msg
	})
}

func (s *RMQService) listen() {
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
	forever := make(chan bool)
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
	<-forever
}

func (s *RMQService) listenReply() {
	msgs, err := s.ch.Consume(
		s.replyQueue,
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
			<-s.replyEvent.Emit(msg.CorrelationId, msg)
		}
	}()
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func randomString(l int) string {
	rand.Seed(time.Now().UnixNano())
	digits := "0123456789"
	specials := "~=+%^*/()[]{}/!@#$?|"
	all := "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
		"abcdefghijklmnopqrstuvwxyz" +
		digits + specials
	buf := make([]byte, l)
	buf[0] = digits[rand.Intn(len(digits))]
	buf[1] = specials[rand.Intn(len(specials))]
	for i := 2; i < l; i++ {
		buf[i] = all[rand.Intn(len(all))]
	}
	rand.Shuffle(len(buf), func(i, j int) {
		buf[i], buf[j] = buf[j], buf[i]
	})
	return string(buf)
}
