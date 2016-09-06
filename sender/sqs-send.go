package main

import (
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: " + os.Args[0] + " <queue name>")
		return
	}

	queueName := os.Args[1]

	svc := sqs.New(session.New(), &aws.Config{Region: aws.String("eu-west-1")})
	resp, err := sendMessage(svc, queueName, false)

	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(resp)
}

func sendMessage(svc *sqs.SQS, queueName string, fromCreateQueue bool) (*sqs.SendMessageOutput, error) {
	params := &sqs.SendMessageInput{
		MessageBody:  aws.String("MESSAGE BODY"),                                                  // Required
		QueueUrl:     aws.String("https://sqs.eu-west-1.amazonaws.com/245915766340/" + queueName), // Required
		DelaySeconds: aws.Int64(0),
	}

	resp, err := svc.SendMessage(params)

	if err == nil {
		return resp, err
	}

	awsErr, ok := err.(awserr.Error)

	if !ok {
		return nil, err
	}

	if "AWS.SimpleQueueService.NonExistentQueue" == awsErr.Code() {
		if fromCreateQueue {
			// To not fall into infinite loop we quit if we're here again
			return nil, err
		}

		params := &sqs.CreateQueueInput{
			QueueName: aws.String(queueName),
		}

		_, err2 := svc.CreateQueue(params)
		if err2 == nil {
			return sendMessage(svc, queueName, true)
		}

		return nil, err2
	}

	return nil, err
}
