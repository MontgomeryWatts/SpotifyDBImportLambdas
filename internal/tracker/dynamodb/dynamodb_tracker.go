package dynamodb

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/MontgomeryWatts/SpotifyDBImportLambdas/internal/tracker"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

var (
	oneDay float64 = 24
)

type DynamoDBTracker struct {
	svc       *dynamodb.DynamoDB
	tableName string
}

func NewDynamoDBTracker() tracker.Tracker {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-east-1"),
	})

	if err != nil {
		log.Fatalf("Error occurred while initializing Session: %v", err)
	}

	return &DynamoDBTracker{
		svc:       dynamodb.New(sess),
		tableName: os.Getenv("TABLE_NAME"),
	}
}

func (ddb *DynamoDBTracker) getTrackingItem(entityURI string) (TrackingItem, error) {
	trackingItem := TrackingItem{}
	result, err := ddb.svc.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(ddb.tableName),
		Key: map[string]*dynamodb.AttributeValue{
			"EntityURI": {
				S: aws.String(entityURI),
			},
		},
	})

	if err != nil {
		return trackingItem, err
	}

	err = dynamodbattribute.UnmarshalMap(result.Item, &trackingItem)

	return trackingItem, err
}

func (ddb *DynamoDBTracker) ArtistIsStale(artistID string) bool {
	trackingItem, err := ddb.getTrackingItem(fmt.Sprintf("spotify:artist:%s", artistID))
	if err != nil {
		log.Fatalf("Error while getting artist status: %v", err)
	}
	if time.Since(trackingItem.LastUpdated).Hours() > (7 * oneDay) {
		return true
	}
	return false
}

func (ddb *DynamoDBTracker) AlbumIsStale(albumID string) bool {
	trackingItem, err := ddb.getTrackingItem(fmt.Sprintf("spotify:album:%s", albumID))
	if err != nil {
		log.Fatalf("Error while getting album status: %v", err)
	}
	if time.Since(trackingItem.LastUpdated).Hours() > (7 * oneDay) {
		return true
	}
	return false
}

func (ddb *DynamoDBTracker) UpdateArtist(artistID string) error {
	return ddb.updateEntity(fmt.Sprintf("spotify:artist:%s", artistID))
}

func (ddb *DynamoDBTracker) UpdateAlbum(albumID string) error {
	return ddb.updateEntity(fmt.Sprintf("spotify:album:%s", albumID))
}

func (ddb *DynamoDBTracker) updateEntity(entityURI string) error {
	trackingItem := TrackingItem{
		EntityURI:   entityURI,
		LastUpdated: time.Now().UTC(),
	}

	itemMap, err := dynamodbattribute.MarshalMap(trackingItem)
	if err != nil {
		return err
	}

	_, err = ddb.svc.PutItem(&dynamodb.PutItemInput{
		Item:      itemMap,
		TableName: aws.String(ddb.tableName),
	})
	return err
}
