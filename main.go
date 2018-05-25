package main

import (
	"io/ioutil"
	"log"
	"os"
	// "path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/fsnotify/fsnotify"
)

// Continuous sync in either direction
// Build manifest and store that remotely.
// option - build manifest every X minutes, or every X events
// Optionally delete things remotely that don't exist locally.
// Exclude/include filters.
// List manifests.
// Restore a directory to a given manifest.
// Sign contents.

func main() {
	source := "/tmp/boo"
	recurse := true

	watcher, _ := fsnotify.NewWatcher()
	watcher.Add(source)
	if recurse {
		files, err := ioutil.ReadDir(source)
		if err != nil {
			log.Fatal(err)
		}

		for _, f := range files {
			if f.IsDir() {
				watcher.Add(source + "/" + f.Name())
			}
		}
	}

	sess, err := session.NewSession()
	if err != nil {
		log.Fatal(err)
	}

	// /tmp/bar/        -> s3://alienthtest/
	// /tmp/bar/foo     -> s3://alienthtest/foo
	// /tmp/bar/foo/bar -> s3://alienthtest/foo/bar

	svc := s3.New(sess)
	for {
		select {
		case event := <-watcher.Events:
			bucket := "alienthtest"
			fi, _ := os.Lstat(event.Name)
			if fi.IsDir() {
				if recurse {
					watcher.Add(event.Name)
				}
				continue
			}
			f, _ := os.Open(event.Name)
			log.Println(event)
			foo := s3.PutObjectInput{
				Bucket: aws.String(bucket),
				Body:   f,
				Key:    aws.String(strings.TrimPrefix(event.Name, source)),
			}
			_, err := svc.PutObject(&foo)
			if err != nil {
				log.Fatal(err)
			}

		}
	}
}
