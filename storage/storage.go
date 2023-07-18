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
)

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

// ListFilesInBucketFolder lists files in a GCP bucket folder.
func ListFilesInBucketFolder(client *storage.Client, ctx context.Context, bucketName, folderPath, delimiter string) ([]string, error) {
	// Get the bucket handle.
	bucket := client.Bucket(bucketName)

	// List files in the specified folder path.
	var files []string
	query := &storage.Query{Prefix: folderPath, Delimiter: delimiter}
	it := bucket.Objects(ctx, query)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error listing objects: %v", err)
		}
		files = append(files, attrs.Name)
	}

	return files, nil
}

// readCSVFileFromGCS reads a CSV file from GCS and sends its content to the dataChannel.
func ReadCSVFileFromGCS(client *storage.Client, ctx context.Context, bucketName, filePath string) ([][]string, error) {
	filePath = replaceLeadingSlashes(filePath)
	path := fmt.Sprintf("gs://%v/%v", bucketName, filePath)
	log.Infof("Loading csv file from %v\n", path)

	// Create a GCS bucket handle
	bucket := client.Bucket(bucketName)

	// Open the CSV file from GCS
	obj := bucket.Object(filePath)
	reader, err := obj.NewReader(ctx)
	if err != nil {
		log.Errorf("Error opening CSV file '%s': %v\n", filePath, err)
		return nil, err
	}
	defer reader.Close()

	// Parse the CSV content
	csvReader := csv.NewReader(reader)
	var data [][]string
	for {
		record, err := csvReader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Errorf("Error reading CSV record from file '%s': %v\n", filePath, err)
			return nil, err
		}
		data = append(data, record)
	}
	log.Debugf("Loaded %v rows from %v", len(data), path)

	return data, nil
}
