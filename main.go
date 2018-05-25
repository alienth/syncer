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

var source location

func main() {
	source = location{Path: "/tmp/boo"}
	source.Manifest = make([]interface{}, 0)
	recurse := true

	watcher, _ := fsnotify.NewWatcher()
	watcher.Add(source.Path)
	if recurse {
		files, err := ioutil.ReadDir(source.Path)
		if err != nil {
			log.Fatal(err)
		}

		for _, f := range files {
			source.Manifest = append(source.Manifest, f)
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
				source.Manifest = append(source.Manifest, fi)
				if recurse {
					watcher.Add(event.Name)
				}
				continue
			}
			destination.handleEvent(event)
		}
	}
}

type location struct {
	Service  interface{}
	Bucket   string
	Path     string
	Manifest []interface{}
}

func (l *location) handleEvent(event fsnotify.Event) {
	switch svc := l.Service.(type) {
	case *s3.S3:
		l.s3HandleEvent(svc, event)
	default:
		log.Fatal("unknown type")
	}

}

func (l *location) s3HandleEvent(svc *s3.S3, event fsnotify.Event) {
	key := aws.String(strings.TrimPrefix(event.Name, source.Path))
	if event.Op == fsnotify.Remove {
		foo := s3.DeleteObjectInput{
			Bucket: aws.String(l.Bucket),
			Key:    key}

		_, err := svc.DeleteObject(&foo)
		if err != nil {
			log.Fatal(err)
		}
	} else if event.Op == fsnotify.Write || event.Op == fsnotify.Create {
		f, _ := os.Open(event.Name)
		foo := s3.PutObjectInput{
			Bucket: aws.String(l.Bucket),
			Body:   f,
			Key:    key,
		}
		_, err := svc.PutObject(&foo)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		log.Println("Ignoring ", event)
	}
}
