package storage

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
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
