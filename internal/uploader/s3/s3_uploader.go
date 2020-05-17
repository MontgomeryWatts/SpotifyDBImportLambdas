package s3

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/MontgomeryWatts/SpotifyDBImportEntityLambda/internal/uploader"
	"github.com/zmb3/spotify"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type S3Uploader struct {
	S3         *s3manager.Uploader
	BucketName string
}

func NewS3Uploader() uploader.Uploader {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-east-1"),
	})

	if err != nil {
		log.Fatalf("Error occurred while initializing Session: %v", err)
	}

	return &S3Uploader{
		S3:         s3manager.NewUploader(sess),
		BucketName: os.Getenv("BUCKET_NAME"),
	}
}

func (up *S3Uploader) UploadArtist(artist *spotify.FullArtist) error {
	key := fmt.Sprintf("artists/%s.json", artist.ID)
	return up.uploadEntity(artist, key)
}

func (up *S3Uploader) UploadAlbum(album *spotify.FullAlbum) error {
	key := fmt.Sprintf("albums/%s.json", album.ID)
	return up.uploadEntity(album, key)
}

func (up *S3Uploader) uploadEntity(entity interface{}, key string) error {
	entityBytes, err := json.Marshal(entity)
	if err != nil {
		return err
	}

	input := &s3manager.UploadInput{
		ContentType: aws.String("application/json"),
		Bucket:      &up.BucketName,
		Key:         &key,
		Body:        bytes.NewBuffer(entityBytes),
	}

	log.Printf("Attempting to insert an entity with key: %s", key)
	_, err = up.S3.Upload(input)
	return err
}
