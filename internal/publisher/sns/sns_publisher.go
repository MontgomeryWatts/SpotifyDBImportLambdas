package sns

import (
	"log"
	"os"

	"github.com/MontgomeryWatts/SpotifyDBImportLambdas/internal/publisher"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
)

type SNSPublisher struct {
	SNS      *sns.SNS
	TopicARN string
}

func NewSNSPublisher() publisher.Publisher {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-east-1"),
	})

	if err != nil {
		log.Fatalf("Error occurred while initializing Session: %v", err)
	}

	return &SNSPublisher{
		SNS:      sns.New(sess),
		TopicARN: os.Getenv("TOPIC_ARN"),
	}
}

func (pub *SNSPublisher) PublishArtistID(ID string) error {
	return pub.publishToTopic("artist", ID)
}

func (pub *SNSPublisher) PublishAlbumID(ID string) error {
	return pub.publishToTopic("album", ID)
}

func (pub *SNSPublisher) publishToTopic(entityType, entityID string) error {
	var attrs = map[string]*sns.MessageAttributeValue{
		"entity_type": &sns.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(entityType),
		},
	}

	log.Printf("Publishing an SNS message for entity type %s with ID: %s", entityType, entityID)
	_, err := pub.SNS.Publish(&sns.PublishInput{
		Message:           aws.String(entityID),
		MessageAttributes: attrs,
		TopicArn:          aws.String(pub.TopicARN),
	})

	return err
}
