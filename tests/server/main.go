package main

import (
	"fmt"

	"github.com/AlariCode/go-rmq"
)

func main() {
	client := rmq.NewRMQService()
	configs := rmq.RMQConfig{
		Host:     "localhost",
		Login:    "guest",
		Password: "guest",
		Exchange: "test",
		Queue:    "go-test",
		Routes: map[string]func([]byte) ([]byte, error){
			"telegramSender.send.rpc": Route,
		},
		PrefetchCount: 10,
	}
	client.Connect(configs)
	client.Listen()
}

type request struct {
	ChatId int64
	Text   string
	Image  []byte
}

type response struct {
	Success bool
}

func Route(body []byte) ([]byte, error) {
	msg, _ := rmq.DecodeMsg[request](body)
	fmt.Println(msg)
	respMsgEncoded, err := rmq.EncodeMsg(response{Success: true})
	if err != nil {
		return nil, err
	}
	return respMsgEncoded, nil
}
