package storage

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/iterator"
	"regexp"
	"time"
)

type BucketObjectLister interface {
	ListObjectsInFolder(client *storage.Client, ctx context.Context, bucketName, foldername string) ([]string, error)
}

// formatPrefixForFolder This function adds / to the end after removing all trailing and leading /
func formatPrefixForFolder(prefix string) string {
	// Define the regular expression pattern to match leading and trailing slashes.
	// The pattern ^/+ matches leading slashes, and /+$ matches trailing slashes.
	pattern := regexp.MustCompile("^/|/$")

	// Use the ReplaceAllString() function to replace the leading and trailing slashes with an empty string.
	prefix = pattern.ReplaceAllString(prefix, "")
	// Add 1 single / at the end
	prefix = prefix + "/"

	return prefix
}

func replaceTrailingSlashes(input string) string {
	// Define the regular expression pattern to match trailing slashes.
	// The pattern /+$ matches one or more trailing slashes.
	pattern := regexp.MustCompile("/+$")

	// Use the ReplaceAllString() function to replace the trailing slashes with an empty string.
	result := pattern.ReplaceAllString(input, "")

	return result
}

func replaceLeadingSlashes(input string) string {
	// Define the regular expression pattern to match leading slashes.
	// The pattern ^/+ matches one or more leading slashes.
	pattern := regexp.MustCompile("^/+")

	// Use the ReplaceAllString() function to replace the leading slashes with an empty string.
	result := pattern.ReplaceAllString(input, "")

	return result
}

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

func ListObjectsInFolder(client *storage.Client, ctx context.Context, bucketName, folderName string) ([]string, error) {
	folderName = formatPrefixForFolder(folderName)
	objAttrs, err := GetObjectsInBucketWithPrefix(client, ctx, bucketName, folderName)
	if err != nil {
		return nil, err
	}
	var objectNames = make([]string, len(objAttrs))
	for _, obj := range objAttrs {
		objectNames = append(objectNames, obj.Name)
	}
	return objectNames, nil
}

func ListObjectsWithPrefix(client *storage.Client, ctx context.Context, bucketName, prefix string) ([]string, error) {
	objAttrs, err := GetObjectsInBucketWithPrefix(client, ctx, bucketName, prefix)
	if err != nil {
		return nil, err
	}
	var objectNames = make([]string, len(objAttrs))
	for _, obj := range objAttrs {
		objectNames = append(objectNames, obj.Name)
	}
	return objectNames, nil
}
