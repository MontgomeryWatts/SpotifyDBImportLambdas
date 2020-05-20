package main

import (
	"log"

	"github.com/MontgomeryWatts/SpotifyDBImportLambdas/internal/publisher"
	"github.com/MontgomeryWatts/SpotifyDBImportLambdas/internal/publisher/sns"
	sp "github.com/MontgomeryWatts/SpotifyDBImportLambdas/internal/spotify"
	"github.com/zmb3/spotify"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
)

func handler(evt events.SQSEvent) {
	client := sp.NewSpotifyClient()
	var publisher publisher.Publisher = sns.NewSNSPublisher()
	outerChan := make(chan bool)
	numSignals := 0

	for _, msg := range evt.Records {
		msgAttrs := msg.MessageAttributes
		ID := msg.Body
		switch entityType := *(msgAttrs["entity_type"].StringValue); entityType {
		case "artist":
			numSignals++
			go func() {
				albums, err := client.GetArtistAlbums(spotify.ID(ID))
				if err != nil {
					log.Fatalf("Unable to get albums for artist with ID %s", ID)
				}
				innerChan := make(chan error, albums.Total)
				for ok := true; ok; ok = (albums.Next != "") {
					for _, album := range albums.Albums {
						albumID := album.ID.String()
						go func() {
							innerChan <- publisher.PublishAlbumID(albumID)
						}()
					}
					client.NextPage(albums)
				}
				for i := 0; i < albums.Total; i++ {
					<-innerChan
				}
				outerChan <- true
			}()
		default:
			log.Fatalf("Unknown entity type received: %s", entityType)
		}
	}

	for signals := 0; signals < numSignals; signals++ {
		<-outerChan
	}
}

func main() {
	lambda.Start(handler)
}
