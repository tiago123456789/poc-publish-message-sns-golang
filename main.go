package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
)

func sendToTopic(
	snsClient *sns.SNS,
	messages []*sns.PublishBatchRequestEntry,
	wg *sync.WaitGroup,
	// sem chan int,
) {
	_, err := snsClient.PublishBatch(&sns.PublishBatchInput{
		TopicArn:                   aws.String(os.Getenv("SNS_TOPIC")),
		PublishBatchRequestEntries: messages,
	})

	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		// <-sem
		wg.Done()
	}()
}

type HTTPClientSettings struct {
	Connect          time.Duration
	ConnKeepAlive    time.Duration
	ExpectContinue   time.Duration
	IdleConn         time.Duration
	MaxAllIdleConns  int
	MaxHostIdleConns int
	ResponseHeader   time.Duration
	TLSHandshake     time.Duration
}

func NewHTTPClientWithSettings(httpSettings HTTPClientSettings) (*http.Client, error) {
	tr := &http.Transport{
		ResponseHeaderTimeout: httpSettings.ResponseHeader,
		Proxy:                 http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			KeepAlive: httpSettings.ConnKeepAlive,
			DualStack: true,
			Timeout:   httpSettings.Connect,
		}).DialContext,
		MaxIdleConns:          httpSettings.MaxAllIdleConns,
		IdleConnTimeout:       httpSettings.IdleConn,
		TLSHandshakeTimeout:   httpSettings.TLSHandshake,
		MaxIdleConnsPerHost:   httpSettings.MaxHostIdleConns,
		ExpectContinueTimeout: httpSettings.ExpectContinue,
	}

	return &http.Client{
		Transport: tr,
	}, nil
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	// httpClient, err := NewHTTPClientWithSettings(HTTPClientSettings{
	// 	Connect:          5 * time.Second,
	// 	ExpectContinue:   1 * time.Second,
	// 	IdleConn:         90 * time.Second,
	// 	ConnKeepAlive:    30 * time.Second,
	// 	MaxAllIdleConns:  100,
	// 	MaxHostIdleConns: 10,
	// 	ResponseHeader:   5 * time.Second,
	// 	TLSHandshake:     5 * time.Second,
	// })

	session, err := session.NewSessionWithOptions(session.Options{
		Profile: os.Getenv("AWS_PROFILE"),
		Config: aws.Config{
			// HTTPClient:                    httpClient,
			Region:                        aws.String(os.Getenv("AWS_REGION")),
			CredentialsChainVerboseErrors: aws.Bool(true),
		},
	})

	if err != nil {
		log.Fatal(err)
	}

	snsClient := sns.New(session)

	// sem := make(chan int, 500)
	fmt.Println("Started process")
	var wg sync.WaitGroup
	timeStart := time.Now()
	var messages []*sns.PublishBatchRequestEntry
	for i := 0; i < 100000; i++ {
		messages = append(messages, &sns.PublishBatchRequestEntry{
			Id:      aws.String(uuid.NewString()),
			Message: aws.String("Hello world"),
		})

		if len(messages) == 10 {
			wg.Add(1)
			// sem <- 1
			go sendToTopic(snsClient, messages, &wg)
			messages = []*sns.PublishBatchRequestEntry{}
		}

	}

	wg.Wait()
	fmt.Println(time.Since(timeStart))
	fmt.Println("Finished process")
}
