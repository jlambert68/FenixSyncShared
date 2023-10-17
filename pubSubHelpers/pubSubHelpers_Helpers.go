package pubSubHelpers

import "github.com/sirupsen/logrus"

func InitiatePubSubFunctionality(tempGcpProject string, loggerReference *logrus.Logger) {
	gcpProject = tempGcpProject
	logger = loggerReference

}
