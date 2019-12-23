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

	for _, msg := range evt.Records {
		msgAttrs := msg.MessageAttributes
		switch entityType := *(msgAttrs["entity_type"].StringValue); entityType {
		case "artist":
			go getAndPutArtist(uploader, client, bucket, msg.Body)
		case "album":
			go getAndPutAlbum(uploader, client, bucket, msg.Body)
		default:
			log.Fatalf("Unknown entity type received: %s", entityType)
		}
	}
}

func main () {
	lambda.Start(Handler)
}

func getAndPutArtist(uploader *s3manager.Uploader, client *spotify.Client, bucket, ID string) {
	artist, err := client.GetArtist(spotify.ID(ID))
	if err != nil {
		log.Printf("Error occurred while retrieving artist with ID: %s\n%v", ID, err)
	} else {
		key := fmt.Sprintf("artists/%s.json", ID)
		artistBytes, err := json.Marshal(artist)
		if err != nil {
			log.Printf("Error occurred while marshalling artist with ID: %s\n%v", ID, err)
		} else {
			body := bytes.NewBuffer(artistBytes)
			input := &s3manager.UploadInput{
				Bucket: aws.String(bucket),
				ContentType: aws.String("application/json"),
				Key: aws.String(key),
				Body: body,
			}
			_, err := uploader.Upload(input)
			if err != nil {
				log.Printf("Failed to upload to S3 for artist with ID: %s\n%v", ID, err)
			}
		}
	}
}

func getAndPutAlbum(uploader *s3manager.Uploader, client *spotify.Client, bucket, ID string) {
	album, err := client.GetAlbum(spotify.ID(ID))
	if err != nil {
		log.Printf("Error occurred while retrieving album with ID: %s\n%v", ID, err)
	} else {
		key := fmt.Sprintf("albums/%s.json", ID)
		albumBytes, err := json.Marshal(album)
		if err != nil {
			log.Printf("Error occurred while marshalling album with ID: %s\n%v", ID, err)
		} else {
			body := bytes.NewBuffer(albumBytes)
			input := &s3manager.UploadInput{
				Bucket: aws.String(bucket),
				ContentType: aws.String("application/json"),
				Key: aws.String(key),
				Body: body,
			}
			_, err := uploader.Upload(input)
			if err != nil {
				log.Printf("Failed to upload to S3 for album with ID: %s\n%v", ID, err)
			}
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