package pubSubHelpers

// Creates a DeadLettering-TopicSubscription
func CreateDeadLetteringTopicSubscription(topicID string) (err error) {

	// Create the Topic-name for DeadLettering
	var deadLetteringTopicName string
	deadLetteringTopicName = CreateDeadLetteringTopicName(topicID)

	// Create the DeadLettingTopic
	err = CreateTopicSubscription(deadLetteringTopicName, "")

	return err
}

// Creates a DeadLettering-Topic-Subscription-Name
func CreateDeadLetteringTopicSubscriptionName(topicID string) (deadLetteringTopicSubscriptionName string) {

	// Create The DeadLettering-Name for the Topic
	var deadLetteringTopicName string
	deadLetteringTopicName = CreateDeadLetteringTopicName(topicID)

	// Create the DeadLettering-Topic-Subscription-name
	deadLetteringTopicSubscriptionName = CreateTopicSubscriptionName(deadLetteringTopicName)

	return deadLetteringTopicSubscriptionName
}
