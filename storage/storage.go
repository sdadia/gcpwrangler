package storage

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/iterator"
	"time"
)

func CreateClientFromBackground() (*storage.Client, context.Context, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("storage.NewClient: %w", err)
	}
	return client, ctx, err
}

func GetBuckets(client *storage.Client, ctx context.Context, projectID string) ([]storage.BucketAttrs, error) {
	log.Debugf("Loading buckets for project : %v\n", projectID)

	ctx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()

	var buckets []storage.BucketAttrs
	it := client.Buckets(ctx, projectID)
	for {
		battrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		buckets = append(buckets, *battrs)
	}
	log.Debugf("Loaded %v buckets for project : %v\n", len(buckets), projectID)

	return buckets, nil
}

func ListBuckets(client *storage.Client, ctx context.Context, projectID string) ([]string, error) {

	// Get bucket atttributes
	bucketAttrs, err := GetBuckets(client, ctx, projectID)
	if err != nil {
		return nil, err
	}

	// Get bucket names
	var bucketNames = make([]string, len(bucketAttrs))
	for _, battrs := range bucketAttrs {
		bucketNames = append(bucketNames, battrs.Name)
	}

	return bucketNames, err
}

func GetObjectsInBucketWithPrefix(client *storage.Client, ctx context.Context, bucketName, prefix string) ([]storage.ObjectAttrs, error) {
	path := fmt.Sprintf("gs://%s/%s", bucketName, prefix)
	log.Debugf("Loading objects in %v\n", path)

	ctx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()

	var objectAttrs []storage.ObjectAttrs
	query := &storage.Query{Prefix: prefix}
	it := client.Bucket(bucketName).Objects(ctx, query)
	for {
		objAttr, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		objectAttrs = append(objectAttrs, *objAttr)
	}
	log.Debugf("Loading %v objects in %v\n", len(objectAttrs), path)

	return objectAttrs, nil
}
