package pubSubHelpers

import (
	"cloud.google.com/go/pubsub"
	"context"
	"errors"
	"github.com/sirupsen/logrus"
)

// Creates a Topic
func createTopic(topicID string) (createdTopic *pubsub.Topic, err error) {

	ctx := context.Background()

	// Create a new PubSub-client
	var pubSubClient *pubsub.Client
	pubSubClient, err = creatNewPubSubClient(ctx)

	if err != nil {

		logger.WithFields(logrus.Fields{
			"ID":           "b5c955cb-2b2b-47e0-a908-1294da40c930",
			"err":          err,
			"pubSubClient": pubSubClient,
		}).Error("Got some problem when creating 'pubsub.NewClient'")

		return nil, err
	}

	if pubSubClient == nil {

		logger.WithFields(logrus.Fields{
			"ID":           "50b55582-70ce-4864-9709-b8bc79fd2382",
			"pubSubClient": pubSubClient,
		}).Error("Got some problem when creating 'pubsub.NewClient'")

		return nil, errors.New("got some problem when creating 'pubsub.NewClient'")
	}

	defer pubSubClient.Close()

	// Create a new Topic
	//var pubSubTopic *pubsub.Topic
	createdTopic, err = pubSubClient.CreateTopic(ctx, topicID)
	if err != nil {

		logger.WithFields(logrus.Fields{
			"ID":  "1ce8e7f5-bbf6-4c9e-9e52-04d292ae0147",
			"err": err,
		}).Error("Got some problem when creating a new PubSub Topic")

		return nil, err
	}

	return createdTopic, err
}
