package main

import (
	"os"
	"fmt"
	"log"
	"bytes"
	"context"
	"encoding/json"

	"github.com/zmb3/spotify"
	"golang.org/x/oauth2/clientcredentials"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
)



func Handler (evt events.SQSEvent) {
	client := newSpotifyClient()
	uploader := newS3Uploader()
	bucket := getEnv("BUCKET_NAME")

	var artistIDs []spotify.ID
	var albumIDs []spotify.ID

	for _, msg := range evt.Records {
		ID := msg.Body
		msgAttrs := msg.MessageAttributes
		switch entityType := *(msgAttrs["entity_type"].StringValue); entityType {
		case "artist":
			artistIDs = append(artistIDs, spotify.ID(ID))
		case "album":
			albumIDs = append(artistIDs, spotify.ID(ID))
		default:
			log.Printf("Unknown entity type received: %s", entityType)
		}
	}

	if artistIDs != nil {
		artists, err := client.GetArtists(artistIDs...)
		if err != nil {
			log.Fatalf("Error occurred while retrieving artists from Spotify\n:%v", err)
		}
		log.Printf("Attempting to insert %d artists", len(artists))
		for _, artist := range artists {
			go putArtist(uploader, artist, bucket)
		}
	}

	if albumIDs != nil {
		albums, err := client.GetAlbums(albumIDs...)
		if err != nil {
			log.Fatalf("Error occurred while retrieving albums from Spotify\n:%v", err)
		}
		log.Printf("Attempting to insert %d albums", len(albums))
		for _, album := range albums {
			go putAlbum(uploader, album, bucket)
		}
	}
}

func main () {
	lambda.Start(Handler)
}

func putArtist(uploader *s3manager.Uploader, artist *spotify.FullArtist, bucket string) {
	key := fmt.Sprintf("artists/%s.json", artist.ID)
	artistBytes, err := json.Marshal(artist)
	if err != nil {
		log.Printf("Error occurred while marshalling artist with ID: %s\n%v", artist.ID, err)
	} else {
		body := bytes.NewBuffer(artistBytes)
		input := &s3manager.UploadInput{
			Bucket: aws.String(bucket),
			ContentType: aws.String("application/json"),
			Key: aws.String(key),
			Body: body,
		}
		log.Printf("Attempting to insert an artist with ID: %s", artist.ID)
		_, err := uploader.Upload(input)
		if err != nil {
			log.Printf("Failed to upload to S3 for artist with ID: %s\n%v", artist.ID, err)
		}
	}
}

func putAlbum(uploader *s3manager.Uploader, album *spotify.FullAlbum, bucket string) {
	key := fmt.Sprintf("albums/%s.json", album.ID)
	albumBytes, err := json.Marshal(album)
	if err != nil {
		log.Printf("Error occurred while marshalling album with ID: %s\n%v", album.ID, err)
	} else {
		body := bytes.NewBuffer(albumBytes)
		input := &s3manager.UploadInput{
			Bucket: aws.String(bucket),
			ContentType: aws.String("application/json"),
			Key: aws.String(key),
			Body: body,
		}
		log.Printf("Attempting to insert an album with ID: %s", album.ID)
		_, err := uploader.Upload(input)
		if err != nil {
			log.Printf("Failed to upload to S3 for album with ID: %s\n%v", album.ID, err)
		}
	}
}

func getEnv(key string) string {
	env, ok := os.LookupEnv(key)
	if !ok {
		log.Fatalf("%s is not set in environment variables", key)
	}
	return env
}

func newSpotifyClient() *spotify.Client {
	config := &clientcredentials.Config{
		ClientID: getEnv("SPOTIFY_ID"),
		ClientSecret: getEnv("SPOTIFY_SECRET"),
		TokenURL: spotify.TokenURL,
	}

	token, err := config.Token(context.Background())
	if err != nil {
		log.Fatalf("Couldn't get token: %v", err)
	}

	client := spotify.Authenticator{}.NewClient(token)
	client.AutoRetry = true
	return &client
}

func newS3Uploader() *s3manager.Uploader {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-east-1"),
	})

	if err != nil {
		log.Fatalf("Error occurred while initializing Session: %v", err)
	}

	return s3manager.NewUploader(sess)
}