package main

import (
	"encoding/json"
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
	}
	client.Connect(configs)
	resp := make(chan []byte)
	req, err := json.Marshal(request{
		ChatId: 1,
		Text:   "test",
		Image:  []byte{},
	})
	if err != nil {
		fmt.Print(err)
		return
	}
	client.Send("telegramSender.send.rpc", req, resp)
	msg, _ := rmq.DecodeMsg[response](<-resp)
	fmt.Println(msg)
}

type request struct {
	ChatId int64
	Text   string
	Image  []byte
}

type response struct {
	Success bool
}
