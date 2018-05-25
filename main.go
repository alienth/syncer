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

// Only syncs directories.

// Continuous sync in either direction
// Build manifest and store that remotely.
// option - build manifest every X minutes, or every X events
// Optionally delete things remotely that don't exist locally.
// Exclude/include filters.
// List manifests.
// Restore a directory to a given manifest.
// Sign contents.

func main() {
	source := location{Path: "/tmp/boo"}
	recurse := true

	watcher, _ := fsnotify.NewWatcher()
	watcher.Add(source.Path)
	if recurse {
		files, err := ioutil.ReadDir(source.Path)
		if err != nil {
			log.Fatal(err)
		}

		for _, f := range files {
			if f.IsDir() {
				watcher.Add(source.Path + "/" + f.Name())
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
	destination := location{Bucket: "alienthtest", Service: svc}
	for {
		select {
		case event := <-watcher.Events:
			fi, _ := os.Lstat(event.Name)
			if fi.IsDir() {
				if recurse {
					watcher.Add(event.Name)
				}
				continue
			}
			destination.handleEvent(source, event)
		}
	}
}

type location struct {
	Service interface{}
	Bucket  string
	Path    string
}

func (l *location) handleEvent(source location, event fsnotify.Event) {
	switch svc := l.Service.(type) {
	case *s3.S3:
		f, _ := os.Open(event.Name)
		foo := s3.PutObjectInput{
			Bucket: aws.String(l.Bucket),
			Body:   f,
			Key:    aws.String(strings.TrimPrefix(event.Name, source.Path)),
		}
		_, err := svc.PutObject(&foo)
		if err != nil {
			log.Fatal(err)
		}
	default:
		log.Fatal("unknown type")
	}

}
