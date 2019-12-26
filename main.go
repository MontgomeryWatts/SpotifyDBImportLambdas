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

	c := make(chan bool)
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
			go putArtist(c, uploader, artist, bucket)
		}
	}

	if albumIDs != nil {
		albums, err := client.GetAlbums(albumIDs...)
		if err != nil {
			log.Fatalf("Error occurred while retrieving albums from Spotify\n:%v", err)
		}
		log.Printf("Attempting to insert %d albums", len(albums))
		for _, album := range albums {
			go putAlbum(c, uploader, album, bucket)
		}
	}

	for i := 0; i < len(albumIDs) + len(artistIDs); i++ {
		<-c
	}
}

func main () {
	lambda.Start(Handler)
}

func putArtist(c chan bool, uploader *s3manager.Uploader, artist *spotify.FullArtist, bucket string) {
	key := fmt.Sprintf("artists/%s.json", artist.ID)
	putEntity(c, uploader, artist, key, bucket)
}

func putAlbum(c chan bool, uploader *s3manager.Uploader, album *spotify.FullAlbum, bucket string) {
	key := fmt.Sprintf("albums/%s.json", album.ID)
	putEntity(c, uploader, album, key, bucket)
}

func putEntity(c chan bool, uploader *s3manager.Uploader, entity interface{}, key, bucket string) {
	entityBytes, err := json.Marshal(entity)
	if err != nil {
		log.Printf("Error occurred while marshalling entity with key: %s\n%v", key, err)
	} else {
		body := bytes.NewBuffer(entityBytes)
		input := &s3manager.UploadInput{
			ContentType: aws.String("application/json"),
			Bucket: &bucket,
			Key: &key,
			Body: body,
		}
		log.Printf("Attempting to insert an entity with key: %s", key)
		result, err := uploader.Upload(input)
		if err != nil {
			log.Printf("Failed to upload to S3 for entity with key: %s\n%v", key, err)
			c<-false
		} else {
			log.Printf("Successfully uploaded entity to S3. %s", result.Location)
			c<-true
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