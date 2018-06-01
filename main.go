package main

import (
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

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

// syncer sync /tmp/boo s3://alienthtest/

var source location
var delete = true

func main() {
	var err error
	source = location{Path: "/tmp/boo"}
	if source.Service, err = os.Lstat(source.Path); err != nil {
		log.Fatal(err)
	}
	source.buildManifest()
	source.listManifest()
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

	sess, err := session.NewSessionWithOptions(session.Options{SharedConfigState: session.SharedConfigEnable})
	if err != nil {
		log.Fatal(err)
	}

	// /tmp/bar/        -> s3://alienthtest/
	// /tmp/bar/foo     -> s3://alienthtest/foo
	// /tmp/bar/foo/bar -> s3://alienthtest/foo/bar

	svc := s3.New(sess)
	destination := location{Bucket: "alienthtest", Service: svc}
	destination.buildManifest()
	destination.listManifest()
	sync(source, destination)
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
			destination.handleEvent(event)
		}
	}
}

type location struct {
	Service  interface{}
	Bucket   string
	Path     string          // the base path which we manage objects from
	Manifest map[string]file // the key is the object relative to the Path
}

func (l *location) handleEvent(event fsnotify.Event) {
	switch event.Op {
	case fsnotify.Write, fsnotify.Create:
		f := constructFile(event)
		key := strings.TrimPrefix(f.Name, l.Path)
		// Need to ensure we retry this if there is a transient failure
		l.Put(key, f)
	case fsnotify.Remove:
		l.Delete(event.Name)
	}

}

func constructFile(event fsnotify.Event) file {
	var f file
	f.Name = filepath.Base(event.Name)
	return f
}

func (l *location) s3HandleEvent(svc *s3.S3, event fsnotify.Event) {
	if event.Op == fsnotify.Remove {
	} else if event.Op == fsnotify.Write || event.Op == fsnotify.Create {
		// l.Put(
	} else {
		log.Println("Ignoring ", event)
	}
}

func (l *location) buildManifest() {
	l.Manifest = make(map[string]file)
	switch svc := l.Service.(type) {
	case *s3.S3:
		foo := s3.ListObjectsV2Input{}
		foo.Bucket = aws.String(l.Bucket)
		foo.Prefix = aws.String(l.Path)
		f := func(list *s3.ListObjectsV2Output, lastPage bool) bool {
			for _, o := range list.Contents {
				var f file
				s3Key := *o.Key
				f.Name = filepath.Base(s3Key)
				f.Path = filepath.Dir(s3Key)
				f.Size = int(*o.Size)
				f.Object = o
				f.LastModified = *o.LastModified
				key := "/" + strings.TrimPrefix(s3Key, l.Path)
				l.Manifest[key] = f
			}

			// Fetch all pages
			return true
		}
		var err error
		if err = svc.ListObjectsV2Pages(&foo, f); err != nil {
			log.Fatal(err)
		}
	case os.FileInfo:
		l.buildDirManifest(l.Path)
	default:
		log.Fatal("unknown type")
	}
}

func (l *location) buildDirManifest(dir string) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		log.Fatal(err)
	}

	for _, fi := range files {
		if fi.IsDir() {
			l.buildDirManifest(dir + "/" + fi.Name())
		} else {
			var f file
			key := dir + "/" + fi.Name()
			key = filepath.Clean(strings.TrimPrefix(key, l.Path))
			f.Name = fi.Name()
			f.Size = int(fi.Size())
			f.Object = fi
			f.Path = dir
			f.LastModified = fi.ModTime()
			l.Manifest[key] = f
		}
	}
}

func (l *location) listManifest() {
	for key, f := range l.Manifest {
		log.Println(key, f.Name, f.Size, f.LastModified)
	}
}

func (l *location) Put(key string, f file) {
	reader := f.Open()
	switch svc := l.Service.(type) {
	case *s3.S3:
		foo := s3.PutObjectInput{
			Bucket: aws.String(l.Bucket),
			Body:   reader,
			Key:    aws.String(key),
		}
		_, err := svc.PutObject(&foo)
		if err != nil {
			log.Fatal(err)
		}
		l.Manifest[key] = f
	default:
		log.Fatal("can't handle")
	}
}

func (l *location) Delete(key string) {
	switch svc := l.Service.(type) {
	case *s3.S3:
		foo := s3.DeleteObjectInput{
			Bucket: aws.String(l.Bucket),
			Key:    aws.String(key)}

		_, err := svc.DeleteObject(&foo)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func sync(source, destination location) {
	// for each object in the source, push it to the destination
	for key, f := range source.Manifest {
		if destF, ok := destination.Manifest[key]; !ok {
			log.Printf("pushing missing %s to destination.", key)
			destination.Put(key, f)
		} else {
			if f.Size != destF.Size {
				log.Printf("pushing mismatched %s to destination.", key)
				destination.Put(key, f)
			}
		}
	}

	// for each object in the destination not in the source, delete it from the destination
	if delete {
		for key, _ := range destination.Manifest {
			if _, ok := source.Manifest[key]; !ok {
				log.Printf("deleting %s from destination.", key)
				destination.Delete(key)
			}
		}
	}
}

type file struct {
	Name         string
	Path         string // the absolute path
	LastModified time.Time
	Size         int
	Object       interface{}
}

func (f *file) Open() io.ReadSeeker {
	switch o := f.Object.(type) {
	case os.FileInfo:
		_ = o
		r, err := os.Open(f.Path + "/" + f.Name)
		if err != nil {
			log.Fatal(err)
		}
		return r
	}
	return nil
}
