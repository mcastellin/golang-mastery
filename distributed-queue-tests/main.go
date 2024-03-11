package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

const numQueues int = 50

const target int = 1000000

const defaultTimeout = 20 * time.Second

const baseUrl = "http://localhost:8080"

func createNamespaces() ([]string, error) {
	cli := &http.Client{
		Timeout: defaultTimeout,
	}

	namespaces := []string{}

	for i := 0; i < numQueues; i++ {
		payload, err := json.Marshal(map[string]any{
			"name": fmt.Sprintf("test-ns-%d", i+1),
		})
		if err != nil {
			return nil, err
		}
		req, err := http.NewRequest("POST", fmt.Sprintf("%s/ns/create", baseUrl), bytes.NewReader(payload))
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
					"namespace":           namespaces[idx],
					"topic":               "test",
					"priority":            1,
					"payload":             "asdfasdfasdf",
					"metadata":            "metadatagasdfasdf",
					"deliverAfterSeconds": 0,
					"ttlSeconds":          300,
				})
				if err != nil {
					fmt.Println(err)
					continue
				}
				req, err := http.NewRequest("POST", fmt.Sprintf("%s/enqueue", baseUrl), bytes.NewReader(payload))
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

	for i := 0; i < len(namespaces); i++ {
		go attackFn(i)
	}

	start := time.Now()
	total := 0
	for {
		cli := http.Client{Timeout: defaultTimeout}
		req, err := http.NewRequest("GET", fmt.Sprintf("%s/dequeue", baseUrl), nil)
		if err != nil {
			fmt.Println(err)
			continue
		}

		response, err := cli.Do(req)
		if err != nil {
			fmt.Println(err)
			continue
		}

		var reply map[string]any
		err = json.NewDecoder(response.Body).Decode(&reply)
		if err != nil {
			fmt.Printf("error decoding response: %v\n", err)
			continue
		}

		messages, ok := reply["messages"]
		if !ok {
			fmt.Println("error enqueuing message", reply)
			continue
		}

		total += len(messages.([]any))
		if total >= target {
			close(closingCh)
			fmt.Println("total messages enqueued:", total)
			totalDuration := time.Since(start)
			fmt.Printf("throughput mess per minute: %.2f\n", float64(total)/totalDuration.Minutes())
			return nil
		}
		if total%1000 == 0 {
			fmt.Println("total messages enqueued:", total)
			totalDuration := time.Since(start)
			fmt.Printf("throughput mess per minute: %.2f\n", float64(total)/totalDuration.Minutes())
		}
	}
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
