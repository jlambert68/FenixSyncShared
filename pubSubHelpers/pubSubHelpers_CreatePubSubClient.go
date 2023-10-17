package pubSubHelpers

import (
	"FenixGuiExecutionServer/common_config"
	"cloud.google.com/go/pubsub"
	"context"
	"crypto/tls"
	"errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func creatNewPubSubClient(ctx context.Context) (pubSubClient *pubsub.Client, err error) {

	// Check that some type of initialization has been done
	if len(gcpProject) == 0 {
		common_config.Logger.WithFields(logrus.Fields{
			"ID":         "6f2e61ea-e768-446c-a8e2-f5d810b37271",
			"gcpProject": gcpProject,
		}).Error("The variable 'gcpProject' is not initialized")

		return nil, errors.New("the variable 'gcpProject' is not initialized")
	}

	projectID := gcpProject

	var opts []grpc.DialOption

	// PubSub is handled within GCP so add TLS
	var creds credentials.TransportCredentials
	creds = credentials.NewTLS(&tls.Config{
		InsecureSkipVerify: true,
	})

	opts = []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
	}

	pubSubClient, err = pubsub.NewClient(ctx, projectID, option.WithGRPCDialOption(opts[0]))

	if err != nil {

		common_config.Logger.WithFields(logrus.Fields{
			"ID":  "2efd364a-5acd-4164-ab89-4bf46ef79b5d",
			"err": err,
		}).Error("Got some problem when creating 'pubsub.NewClient'")

		return nil, err
	}

	return pubSubClient, err
}
