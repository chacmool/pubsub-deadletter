### Usage

go run main.go -project yokesenosoycientifico -topic imaya -subscription sub -deadtopic imaya-deadletter -deadsubscription deadsub


### Example of received info on deadletter subscription using pubsub receiver
```
Attributes:CloudPubSubDeadLetterSourceDeliveryCount = 5
ce-type = com.cloudevents.sample.sent
ce-specversion = 1.0
CloudPubSubDeadLetterSourceSubscription = sub
CloudPubSubDeadLetterSourceSubscriptionProject = yokesenosoycientifico
ce-source = github.com/cloudevents/sdk-go/samples/pubsub/sender/
ce-id = 4d6c96e1-df94-4aa8-9a14-efbc91071920
CloudPubSubDeadLetterSourceTopicPublishTime = 2023-09-13T09:51:21.34+00:00
Content-Type = application/json
```

### Links
https://cloud.google.com/pubsub/docs/handling-failures

https://stackoverflow.com/questions/63165028/how-does-the-exponential-backoff-configured-in-google-pub-subs-retrypolicy-work