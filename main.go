package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	cepubsub "github.com/cloudevents/sdk-go/protocol/pubsub/v2"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/peterbourgon/ff/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Data struct {
	Message string `json:"message"`
}

func main() {
	fs := flag.NewFlagSet("receiver", flag.ContinueOnError)
	var (
		projectID          = fs.String("project", "", "")
		topicID            = fs.String("topic", "", "")
		subscriptionID     = fs.String("subscription", "", "")
		deadTopicID        = fs.String("deadtopic", "", "")
		deadSubscriptionID = fs.String("deadsubscription", "", "")
	)
	if err := ff.Parse(fs, os.Args[1:], ff.WithEnvVarNoPrefix()); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	log.Printf("ProjectID %s, TopicID %s, SubscriptionID %s, DeadTopicID %s, DeadSubscriptionID %s\n", *projectID, *topicID, *subscriptionID, *deadTopicID, *deadSubscriptionID)

	wg := new(sync.WaitGroup)
	wg.Add(1)
	ctx := context.Background()

	pubsubClient, err := pubsub.NewClient(ctx, *projectID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer pubsubClient.Close()

	ok, err := pubsubClient.Subscription(*subscriptionID).Exists(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	if !ok {
		_, err := pubsubClient.CreateSubscription(ctx, *subscriptionID, pubsub.SubscriptionConfig{
			Topic: pubsubClient.Topic(*topicID),
			RetryPolicy: &pubsub.RetryPolicy{
				MinimumBackoff: 1 * time.Second,
				MaximumBackoff: 2 * time.Second,
			},
			DeadLetterPolicy: &pubsub.DeadLetterPolicy{
				DeadLetterTopic:     pubsubClient.Topic(*deadTopicID).String(),
				MaxDeliveryAttempts: 5,
			},
		})
		if err != nil && status.Code(err) != codes.AlreadyExists {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
	}

	t, err := cepubsub.New(ctx,
		cepubsub.WithClient(pubsubClient),
		cepubsub.WithSubscriptionAndTopicID(*subscriptionID, *topicID),
	)
	if err != nil {
		log.Fatalf("failed to create pubsub protocol, %s", err.Error())
	}
	c, err := cloudevents.NewClient(t)

	if err != nil {
		log.Fatalf("failed to create client, %s", err.Error())
	}

	log.Println("Created client, listening...")

	go func() {
		defer t.Close(ctx)
		if err := c.StartReceiver(ctx, func(ctx context.Context, event event.Event) error {
			data := &Data{}
			if err := event.DataAs(data); err != nil {
				fmt.Printf("Got Data Error: %s\n", err.Error())
			}
			log.Printf("Data: %v\n Event ID: %v\n", data, event.ID())
			return errors.New("processor error")
		}); err != nil {
			log.Fatalf("failed to start pubsub receiver, %s", err.Error())
		}
	}()

	// dead letter listener
	dLPubsubClient, err := pubsub.NewClient(ctx, *projectID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer dLPubsubClient.Close()

	dLProtocol, err := cepubsub.New(ctx,
		cepubsub.WithClient(dLPubsubClient),
		cepubsub.WithSubscriptionAndTopicID(*deadSubscriptionID, *deadTopicID),
	)
	if err != nil {
		log.Fatalf("failed to create pubsub dLProtocol, %s", err.Error())
	}
	dLClient, err := cloudevents.NewClient(dLProtocol)

	if err != nil {
		log.Fatalf("failed to create dLClient, %s", err.Error())
	}

	log.Println("Created dLClient, listening...")

	go func() {
		defer dLProtocol.Close(ctx)
		if err := dLClient.StartReceiver(ctx, func(ctx context.Context, event event.Event) error {
			data := &Data{}
			if err := event.DataAs(data); err != nil {
				fmt.Printf("DEAD LETTER Got Data Error: %s\n", err.Error())
			}
			log.Printf("DEAD LETTER: %v \n Event ID: %v \n", data, event.ID())
			log.Printf("DEAD LETTER: Event Context %s \n ", event.Context.String())
			return nil
		}); err != nil {
			log.Fatalf("failed to start pubsub receiver, %s", err.Error())
		}
	}()

	// dead letter listener (pubsub)
	// dLPubsubDirectClient, err := pubsub.NewClient(ctx, *projectID)
	// if err != nil {
	// 	fmt.Fprintf(os.Stderr, "error: %v\n", err)
	// 	os.Exit(1)
	// }
	// defer dLPubsubDirectClient.Close()
	// sub := dLPubsubDirectClient.Subscription(*deadSubscriptionID)

	// go func() {
	// 	err = sub.Receive(ctx, func(_ context.Context, msg *pubsub.Message) {
	// 		log.Printf("Got message :%q\n", string(msg.Data))
	// 		fmt.Printf("Attributes:")
	// 		for key, value := range msg.Attributes {
	// 			fmt.Printf("%s = %s\n", key, value)
	// 		}
	// 		msg.Ack()
	// 	})
	// 	if err != nil {
	// 		fmt.Printf("sub.Receive: %v", err)
	// 		os.Exit(1)
	// 	}
	// }()

	time.Sleep(5 * time.Second)

	//sender
	senderPubsubClient, err := pubsub.NewClient(ctx, *projectID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer senderPubsubClient.Close()
	senderProtocol, err := cepubsub.New(ctx, cepubsub.WithClient(senderPubsubClient), cepubsub.WithTopicID(*topicID))
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	senderClient, err := cloudevents.NewClient(senderProtocol, cloudevents.WithUUIDs())
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	event := cloudevents.NewEvent()
	event.SetType("com.cloudevents.sample.sent")
	event.SetSource("github.com/cloudevents/sdk-go/samples/pubsub/sender/")
	_ = event.SetData("application/json", &Data{
		Message: "hi!",
	})

	if result := senderClient.Send(ctx, event); cloudevents.IsUndelivered(result) {
		log.Printf("result: %v", result)
		log.Printf("failed to send: %v", err)
	} else {
		log.Printf("sent, accepted: %t", cloudevents.IsACK(result))
	}

	wg.Wait()
}
