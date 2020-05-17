package main

import (
	"log"

	"github.com/MontgomeryWatts/SpotifyDBImportEntityLambda/internal/publisher"
	"github.com/MontgomeryWatts/SpotifyDBImportEntityLambda/internal/publisher/sns"
	sp "github.com/MontgomeryWatts/SpotifyDBImportEntityLambda/internal/spotify"
	"github.com/zmb3/spotify"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
)

func handler(evt events.SQSEvent) {
	client := sp.NewSpotifyClient()
	var publisher publisher.Publisher
	publisher = sns.NewSNSPublisher()

	c := make(chan bool)
	receives := 0

	for _, msg := range evt.Records {
		msgAttrs := msg.MessageAttributes
		ID := msg.Body
		switch entityType := *(msgAttrs["entity_type"].StringValue); entityType {
		case "artist":
			albums, err := client.GetArtistAlbums(spotify.ID(ID))
			if err != nil {
				log.Fatalf("Unable to get albums for artist with ID %s", ID)
			}
			receives++
			go publishAlbumIDsToTopic(c, client, albums, svc, topicArn)
		default:
			log.Fatalf("Unknown entity type received: %s", entityType)
		}
	}

	for i := 0; i < receives; i++ {
		<-c
	}
}

func main() {
	lambda.Start(handler)
}
