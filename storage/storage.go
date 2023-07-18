package storage

import (
	"cloud.google.com/go/storage"
	"context"
	"encoding/csv"
	"fmt"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/iterator"
	"io"
	"regexp"
	"time"
)

type ReadCSV interface {
	ReadCSV() ([][]string, error)
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

func ListBuckets(client *storage.Client, ctx context.Context, projectID string) ([]string, error) {
	log.Debugf("Loading buckets for project : %v\n", projectID)

	ctx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()

	var buckets []string
	it := client.Buckets(ctx, projectID)
	for {
		battrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		buckets = append(buckets, battrs.Name)
	}
	log.Debugf("Loaded %v buckets for project : %v\n", len(buckets), projectID)

	return buckets, nil
}

func ListFiles(client *storage.Client, ctx context.Context, bucketName, prefix string) ([]string, error) {
	path := fmt.Sprintf("gs://%s/%s", bucketName, prefix)
	log.Debugf("Loading objects in %v\n", path)

	ctx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()

	var fileNames []string
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
		fileNames = append(fileNames, objAttr.Name)
	}
	log.Debugf("Loading %v objects in %v\n", len(fileNames), path)

	return fileNames, nil
}

func ReadCSVFile(client *storage.Client, ctx context.Context, bucketName, fileName string) ([][]string, error) {
	log.Debugf("Loading csv file gs://%v/%v", bucketName, fileName)

	// Open the bucket and object.
	bucket := client.Bucket(bucketName)
	obj := bucket.Object(fileName)

	// Read the object from GCS.
	r, err := obj.NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to open GCS object: %v", err)
	}
	defer r.Close()

	// Parse the CSV data.
	reader := csv.NewReader(r)
	var data [][]string

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error reading CSV: %v", err)
		}
		data = append(data, record)
	}
	log.Debugf("Loaded %v rows from csv file gs://%v/%v", len(data), bucketName, fileName)

	return data, nil
}
