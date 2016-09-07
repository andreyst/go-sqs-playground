package main

import (
	"bytes"
	"fmt"
	"log"
	"os/exec"
	"reflect"
	"strings"
	"sync"
	"time"

	"menteslibres.net/gosexy/yaml"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func main() {
	var wg sync.WaitGroup
	wg.Add(1)

	settings, err := yaml.Open("config.yml")
	if err != nil {
		log.Fatalf("Cannot read config: %v\n", err.Error())
		return
	}

	svc := createSvc()

	err2 := launchLoops(settings, svc)
	if err2 != nil {
		log.Fatalf(err2.Error())
		return
	}

	wg.Wait()
}

func createSvc() (service *sqs.SQS) {
	svc := sqs.New(session.New(), &aws.Config{Region: aws.String("eu-west-1")})

	return svc
}

func launchLoops(settings *yaml.Yaml, svc *sqs.SQS) error {
	queues := settings.Get("queues")

	queuesType := reflect.TypeOf(queues).Kind()
	if queuesType != reflect.Slice {
		return fmt.Errorf("Queues config entry should be an array of strings, it is %v", queuesType)
	}

	for _, queue := range queues.([]interface{}) {
		queueType := reflect.TypeOf(queue).Kind()
		if queueType != reflect.String {
			return fmt.Errorf("Queue entry should be a string, it is %v", queueType)
		}
		go receiveMessageLoop(svc, queue.(string))
	}

	return nil
}

func receiveMessageLoop(svc *sqs.SQS, queueName string) {
	for {
		processMessage(svc, queueName)
	}
}

func processMessage(svc *sqs.SQS, queueName string) {
	message := receiveMessage(svc, queueName)
	if message == nil {
		return
	}

	runWorker(*message.Body, queueName)
	deleteMessage(svc, message, queueName)
}

func receiveMessage(svc *sqs.SQS, queueName string) (message *sqs.Message) {
	log.Println("[" + queueName + "] Waiting for new messages... ")

	receiveMessageParams := &sqs.ReceiveMessageInput{
		QueueUrl:          aws.String("https://sqs.eu-west-1.amazonaws.com/245915766340/" + queueName), // Required
		VisibilityTimeout: aws.Int64(100),
		WaitTimeSeconds:   aws.Int64(20),
	}

	resp, err := svc.ReceiveMessage(receiveMessageParams)

	if err != nil {
		log.Fatal(err)
		return
	}

	if len(resp.Messages) < 1 {
		log.Println("[" + queueName + "] No new messages")
		return
	}

	log.Println("[" + queueName + "] Got new message")

	return resp.Messages[0]
}

func deleteMessage(svc *sqs.SQS, message *sqs.Message, queueName string) {
	deleteMessageParams := &sqs.DeleteMessageInput{
		QueueUrl:      aws.String("https://sqs.eu-west-1.amazonaws.com/245915766340/" + queueName), // Required
		ReceiptHandle: message.ReceiptHandle,
	}
	svc.DeleteMessage(deleteMessageParams)
}

func runWorker(payload string, queueName string) {
	start := time.Now()

	log.Printf("[" + queueName + "] Running worker\n")

	cmd := exec.Command("php", "worker.php")
	var out bytes.Buffer
	cmd.Stdin = strings.NewReader(payload)
	cmd.Stdout = &out
	err2 := cmd.Run()
	if err2 != nil {
		log.Fatal(err2)
	}
	log.Printf("["+queueName+"] Worker output: %v", out.String())

	timeSpent := time.Since(start)
	log.Printf("["+queueName+"] Ran worker in %.3f\n", timeSpent.Seconds())
}
