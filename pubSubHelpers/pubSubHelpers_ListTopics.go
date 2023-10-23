package pubSubHelpers

import (
	"cloud.google.com/go/pubsub"
	"context"
	"errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/api/iterator"
)

// List Topics
func ListTopics() (pubSubTopics []*pubsub.Topic, err error) {

	ctx := context.Background()

	// Create a new PubSub-client
	var pubSubClient *pubsub.Client
	pubSubClient, err = creatNewPubSubClient(ctx)

	if err != nil {

		logger.WithFields(logrus.Fields{
			"ID":  "358e33c0-f993-4ce6-95e1-538bd14c466b",
			"err": err,
		}).Error("Got some problem when creating 'pubsub.NewClient'")

		return nil, err
	}

	defer pubSubClient.Close()

	// Get Topics
	var pubSubTopicIterator *pubsub.TopicIterator
	pubSubTopicIterator = pubSubClient.Topics(ctx)
	for {
		var pubSubTopic *pubsub.Topic
		pubSubTopic, err = pubSubTopicIterator.Next()
		if errors.Is(err, iterator.Done) {

			// Clear the error before leaving
			err = nil

			break
		}
		if err != nil {

			logger.WithFields(logrus.Fields{
				"ID":  "2029f0b4-be98-4057-adf9-911147adfce1",
				"err": err,
			}).Error("Got some problem iterating the topics-response")

			return nil, err
		}
		pubSubTopics = append(pubSubTopics, pubSubTopic)
	}

	return pubSubTopics, err

}
