package main

import (
	"bytes"
	"fmt"
	"log"
	"os/exec"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func main() {
	var wg sync.WaitGroup
	wg.Add(1)

	svc := sqs.New(session.New(), &aws.Config{Region: aws.String("eu-west-1")})
	go receiveMessageLoop(svc, "TestQueue")
	go receiveMessageLoop(svc, "TestQueue2")
	wg.Wait()
}

func receiveMessageLoop(svc *sqs.SQS, queueName string) {

	for {
		receiveMessage(svc, queueName)
	}
}

func receiveMessage(svc *sqs.SQS, queueName string) {

	log.Println("Receiving new messages... " + queueName)

	receiveMessageParams := &sqs.ReceiveMessageInput{
		QueueUrl:          aws.String("https://sqs.eu-west-1.amazonaws.com/245915766340/" + queueName), // Required
		VisibilityTimeout: aws.Int64(100),
		WaitTimeSeconds:   aws.Int64(10),
	}

	resp, err := svc.ReceiveMessage(receiveMessageParams)

	if err != nil {
		log.Fatal(err)
		return
	}

	if len(resp.Messages) < 1 {
		log.Println("No new messages in " + queueName)
		return
	}

	log.Println("Got new message from " + queueName)

	message := resp.Messages[0]

	cmd := exec.Command("php", "worker.php")
	var out bytes.Buffer
	cmd.Stdin = strings.NewReader(*message.Body)
	cmd.Stdout = &out
	err2 := cmd.Run()
	if err2 != nil {
		log.Fatal(err2)
	}
	fmt.Printf("%v", out.String())

	deleteMessageParams := &sqs.DeleteMessageInput{
		QueueUrl:      aws.String("https://sqs.eu-west-1.amazonaws.com/245915766340/" + queueName), // Required
		ReceiptHandle: message.ReceiptHandle,
	}
	svc.DeleteMessage(deleteMessageParams)
}
