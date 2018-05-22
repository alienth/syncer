package main

import (
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/fsnotify/fsnotify"
)

// Continuous sync in either direction
// Build manifest and store that remotely.
// Optionally delete things remotely that don't exist locally.
// Exclude/include filters.
// List manifests.
// Restore a directory to a given manifest.
// Sign contents.

func main() {
	watcher, _ := fsnotify.NewWatcher()
	watcher.Add("/tmp")

	sess, err := session.NewSession()
	if err != nil {
		log.Fatal(err)
	}

	svc := s3.New(sess)
	for {
		select {
		case event := <-watcher.Events:
			bucket := "s3://alienth"
			f, _ := os.Open(event.Name)
			foo := s3.PutObjectInput{
				Bucket: &bucket,
				Body:   f,
			}
			log.Println(foo, svc)

		}
	}
}
