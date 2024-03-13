package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"time"
)

const numNamespaces int = 1

const target int = 1000000

const defaultTimeout = 20 * time.Second

const baseUrl = "http://localhost:8080"

var topics = []string{"test", "one", "there-it-is", "one-two-three-zero", "another-one", "done", "my", "research", "on", "this", "one", "is", "very", "nice", "and", "tasty", "task"}

var (
	urlCreateNs       = fmt.Sprintf("%s/ns", baseUrl)
	urlGetNs          = fmt.Sprintf("%s/ns", baseUrl)
	urlEnqueueMessage = fmt.Sprintf("%s/message/enqueue", baseUrl)
	urlDequeueMessage = fmt.Sprintf("%s/message/dequeue", baseUrl)
	urlAckMessage     = fmt.Sprintf("%s/message/ack", baseUrl)
)

func createNamespaces() ([]string, error) {
	cli := &http.Client{
		Timeout: defaultTimeout,
	}

	namespaces := []string{}

	for i := 0; i < numNamespaces; i++ {
		payload, err := json.Marshal(map[string]any{
			"name": fmt.Sprintf("test-ns-%d", i+1),
		})
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

		var reply map[string]string
		err = json.NewDecoder(response.Body).Decode(&reply)
		if err != nil {
			return nil, err
		}

		namespaces = append(namespaces, reply["id"])
	}

	return namespaces, nil
}

func attack(namespaces []string) error {
	closingCh := make(chan struct{})

	attackFn := func(idx int) {
		cli := http.Client{Timeout: defaultTimeout}
		for {
			select {
			case <-closingCh:
				return
			default:
				payload, err := json.Marshal(map[string]any{
					"namespace":           namespaces[0],
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
		go consumer(notifyCh, i)
	}

	start := time.Now()
	total := 0

	checkpoint := 100
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

func consumer(notifyCh chan<- int, topicIdx int) {
	for {
		body := struct {
			Namespace      string `json:"namespace"`
			Topic          string `json:"topic"`
			Limit          int    `json:"limit"`
			TimeoutSeconds int    `json:"timeoutSeconds"`
		}{"ns", topics[topicIdx], 10, 20}

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

func main() {

	namespaces, err := createNamespaces()
	if err != nil {
		panic(err)
	}

	fmt.Println("created namespaces")
	err = attack(namespaces)
	if err != nil {
		panic(err)
	}
}
