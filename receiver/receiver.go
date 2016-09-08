package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"time"

	"menteslibres.net/gosexy/yaml"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func main() {
	wg := new(sync.WaitGroup)

	settings, err := yaml.Open("config.yml")
	if err != nil {
		log.Fatalf("Cannot read config: %v\n", err.Error())
		return
	}

	svc := createSvc()

	shutdownChans, err2 := launchLoops(wg, settings, svc)
	if err2 != nil {
		log.Fatalf(err2.Error())
		return
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go signalProcessor(shutdownChans, sigs, wg)

	wg.Wait()
}

func signalProcessor(shutdownChans []chan *sync.WaitGroup, sigs chan os.Signal, wg *sync.WaitGroup) {
	repeatTimeout := 10 * time.Second
	caughtSignalTimes := 0
	prevSignalCaughtAt, _ := time.Parse("2006-Jan-02", "0001-Jan-01")

	for {
		<-sigs

		fmt.Println()

		if time.Since(prevSignalCaughtAt) > repeatTimeout {
			caughtSignalTimes = 0
		}

		caughtSignalTimes++
		if caughtSignalTimes > 1 {
			fmt.Println("Aborting")
			for i := 1; i <= len(shutdownChans); i++ {
				wg.Done()
			}
			return
		}

		fmt.Printf("Shutting down gracefully, repeat signal in %v to abort...\n", repeatTimeout)
		for _, shutdownChan := range shutdownChans {
			select {
			case shutdownChan <- wg:
			default:
			}
		}

		prevSignalCaughtAt = time.Now()
	}
}

func createSvc() (service *sqs.SQS) {
	svc := sqs.New(session.New(), &aws.Config{Region: aws.String("eu-west-1")})

	return svc
}

func launchLoops(wg *sync.WaitGroup, settings *yaml.Yaml, svc *sqs.SQS) (chans []chan *sync.WaitGroup, err error) {
	queues := settings.Get("queues")

	queuesType := reflect.TypeOf(queues).Kind()
	if queuesType != reflect.Slice {
		return nil, fmt.Errorf("Queues config entry should be an array of strings, it is %v", queuesType)
	}

	var queueNames []string
	var shutdownChans []chan *sync.WaitGroup

	for _, queueName := range queues.([]interface{}) {
		queueType := reflect.TypeOf(queueName).Kind()
		if queueType != reflect.String {
			return nil, fmt.Errorf("Queue entry should be a string, it is %v", queueType)
		}

		queueNames = append(queueNames, queueName.(string))
	}

	for _, queueName := range queueNames {
		shutdownChan := make(chan *sync.WaitGroup, 1)
		shutdownChans = append(shutdownChans, shutdownChan)
		go receiveMessageLoop(shutdownChan, svc, queueName)
		wg.Add(1)
	}

	return shutdownChans, nil
}

func receiveMessageLoop(shutdownChan chan *sync.WaitGroup, svc *sqs.SQS, queueName string) {
	for {
		select {
		case wg := <-shutdownChan:
			log.Println("[" + queueName + "] Got shutdown signal from chan, shutting down")
			wg.Done()
			return
		default:
		}
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
