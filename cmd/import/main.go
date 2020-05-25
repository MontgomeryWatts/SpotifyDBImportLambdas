package main

import (
	"log"

	sp "github.com/MontgomeryWatts/SpotifyDBImportLambdas/internal/spotify"
	"github.com/MontgomeryWatts/SpotifyDBImportLambdas/internal/tracker"
	"github.com/MontgomeryWatts/SpotifyDBImportLambdas/internal/tracker/dynamodb"
	"github.com/MontgomeryWatts/SpotifyDBImportLambdas/internal/uploader"
	"github.com/MontgomeryWatts/SpotifyDBImportLambdas/internal/uploader/s3"
	"github.com/zmb3/spotify"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
)

func handler(evt events.SQSEvent) {
	client := sp.NewSpotifyClient()
	var artistIDs []spotify.ID
	var albumIDs []spotify.ID
	var uploader uploader.Uploader = s3.NewS3Uploader()
	var tracker tracker.Tracker = dynamodb.NewDynamoDBTracker()

	for _, msg := range evt.Records {
		ID := msg.Body
		msgAttrs := msg.MessageAttributes
		switch entityType := *(msgAttrs["entity_type"].StringValue); entityType {
		case "artist":
			artistIDs = append(artistIDs, spotify.ID(ID))
		case "album":
			albumIDs = append(albumIDs, spotify.ID(ID))
		default:
			log.Printf("Unknown entity type received: %s", entityType)
		}
	}

	numEntities := len(artistIDs) + len(albumIDs)
	c := make(chan error, numEntities)

	if artistIDs != nil {
		artists, err := client.GetArtists(artistIDs...)
		if err != nil {
			log.Fatalf("Error occurred while retrieving artists from Spotify\n:%v", err)
		}
		log.Printf("Attempting to insert %d artists", len(artists))
		for _, artist := range artists {
			go func(arg *spotify.FullArtist) {
				err = uploader.UploadArtist(arg)
				if err != nil {
					c <- err
				} else {
					c <- tracker.UpdateArtist(arg.ID.String())
				}
			}(artist)
		}
	}

	if albumIDs != nil {
		albums, err := client.GetAlbums(albumIDs...)
		if err != nil {
			log.Fatalf("Error occurred while retrieving albums from Spotify\n:%v", err)
		}
		log.Printf("Attempting to insert %d albums", len(albums))
		for _, album := range albums {
			go func(arg *spotify.FullAlbum) {
				err = uploader.UploadAlbum(arg)
				if err != nil {
					c <- err
				} else {
					c <- tracker.UpdateAlbum(arg.ID.String())
				}
			}(album)
		}
	}

	for i := 0; i < numEntities; i++ {
		if err := <-c; err != nil {
			log.Print(err)
		}
	}
}

func main() {
	lambda.Start(handler)
}
