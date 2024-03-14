package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"strings"
	"time"
)

const (
	target         int = 1000000
	defaultTimeout     = 20 * time.Second
	baseUrl            = "http://localhost:8080"
	numTopics          = 50
)

var (
	urlCreateNs       = fmt.Sprintf("%s/ns", baseUrl)
	urlGetNs          = fmt.Sprintf("%s/ns", baseUrl)
	urlEnqueueMessage = fmt.Sprintf("%s/message/enqueue", baseUrl)
	urlDequeueMessage = fmt.Sprintf("%s/message/dequeue", baseUrl)
	urlAckMessage     = fmt.Sprintf("%s/message/ack", baseUrl)
)

func main() {

	ns := "default"
	nsId, err := createNamespace(ns)
	if err != nil {
		panic(err)
	}

	topics := make([]string, numTopics)
	for i := 0; i < numTopics; i++ {
		topics[i] = generateName(50)
	}

	if err := attack(*nsId, topics); err != nil {
		panic(err)
	}
}

func createNamespace(name string) (*string, error) {
	cli := &http.Client{
		Timeout: defaultTimeout,
	}

	payload, err := json.Marshal(map[string]any{"name": name})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodPost, urlCreateNs, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	response, err := cli.Do(req)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != 200 {
		return nil, fmt.Errorf("error creating namespace")
	}

	var reply map[string]string
	err = json.NewDecoder(response.Body).Decode(&reply)
	if err != nil {
		return nil, err
	}

	nsId := reply["id"]
	return &nsId, nil
}

func attack(namespace string, topics []string) error {
	closingCh := make(chan struct{})

	attackFn := func(idx int) {
		cli := http.Client{Timeout: defaultTimeout}
		for {
			select {
			case <-closingCh:
				return
			default:
				payload, err := json.Marshal(map[string]any{
					"namespace":           namespace,
					"topic":               topics[idx],
					"priority":            rand.Intn(100),
					"payload":             "asdfasdfasdf",
					"metadata":            "metadatagasdfasdf",
					"deliverAfterSeconds": rand.Intn(10),
					"ttlSeconds":          300,
				})
				if err != nil {
					fmt.Println(err)
					continue
				}
				req, err := http.NewRequest(http.MethodPost, urlEnqueueMessage, bytes.NewReader(payload))
				if err != nil {
					fmt.Println(err)
					continue
				}

				response, err := cli.Do(req)
				if err != nil {
					fmt.Println(err)
					continue
				}

				var reply map[string]string
				err = json.NewDecoder(response.Body).Decode(&reply)
				if err != nil {
					continue
				}

				if _, ok := reply["msgId"]; !ok {
					fmt.Println("error enqueuing message", reply)
					continue
				}
			}
		}
	}

	notifyCh := make(chan int)
	for i := 0; i < len(topics); i++ {
		go attackFn(i)
		go consumer(notifyCh, namespace, topics[i])
	}

	start := time.Now()
	total := 0

	checkpoint := 1000
	for count := range notifyCh {
		total += count
		if total >= target {
			close(closingCh)
			fmt.Println("total messages processed:", total)
			totalDuration := time.Since(start)
			fmt.Printf("throughput mess per minute: %.2f\n", float64(total)/totalDuration.Minutes())
			return nil
		}
		if total > checkpoint {
			fmt.Println("total messages processed:", total)
			totalDuration := time.Since(start)
			fmt.Printf("throughput mess per minute: %.2f\n", float64(total)/totalDuration.Minutes())
			checkpoint += 1000
		}
	}

	return nil
}

func consumer(notifyCh chan<- int, namespace, topic string) {
	for {
		body := struct {
			Namespace      string `json:"namespace"`
			Topic          string `json:"topic"`
			Limit          int    `json:"limit"`
			TimeoutSeconds int    `json:"timeoutSeconds"`
		}{namespace, topic, 10, 20}

		payload, _ := json.Marshal(body)
		cli := http.Client{Timeout: 30 * time.Second}
		req, err := http.NewRequest(http.MethodPost, urlDequeueMessage, bytes.NewReader(payload))
		if err != nil {
			fmt.Println(err)
			continue
		}

		response, err := cli.Do(req)
		if err != nil {
			fmt.Println(err)
			continue
		}

		var reply struct {
			Messages []struct {
				Id string `json:"id"`
			} `json:"messages"`
		}
		err = json.NewDecoder(response.Body).Decode(&reply)
		if err != nil {
			fmt.Printf("error decoding response: %v\n", err)
			continue
		}

		for _, m := range reply.Messages {
			ack(m.Id, true)
		}
		if len(reply.Messages) > 0 {
			notifyCh <- len(reply.Messages)
		}
	}
}

func ack(msgId string, v bool) {
	cli := http.Client{Timeout: defaultTimeout}
	ack := []struct {
		Id  string `json:"id"`
		Ack bool   `json:"ack"`
	}{{Id: msgId, Ack: v}}
	body, err := json.Marshal(&ack)
	if err != nil {
		fmt.Println(err)
	}
	req, err := http.NewRequest(http.MethodPost, urlAckMessage, bytes.NewReader(body))
	if err != nil {
		fmt.Println(err)
	}
	cli.Do(req)
}

// generateName creates a random string of a specified length.
func generateName(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	var output strings.Builder
	for i := 0; i < length; i++ {
		randomIndex := rand.Intn(len(charset))
		randomChar := charset[randomIndex]
		output.WriteByte(randomChar)
	}
	return output.String()
}
