package storage

import (
	"cloud.google.com/go/storage"
	"context"
	"encoding/csv"
	"fmt"
	"github.com/maruel/natural"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/iterator"

	"io"
	"regexp"
	"sort"
	"time"
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

type objectByCreationTime []*storage.ObjectAttrs

func (o objectByCreationTime) Len() int           { return len(o) }
func (o objectByCreationTime) Less(i, j int) bool { return o[i].Updated.Before(o[j].Updated) }
func (o objectByCreationTime) Swap(i, j int)      { o[i], o[j] = o[j], o[i] }

func ListFiles(client *storage.Client, ctx context.Context, bucketName, prefix string, sortBy string) ([]string, error) {
	path := fmt.Sprintf("gs://%s/%s", bucketName, prefix)
	log.Debugf("Getting objects in %v\n", path)

	ctx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()

	objects := make([]*storage.ObjectAttrs, 0)
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
		objects = append(objects, objAttr)
	}
	log.Debugf("Got %v objects in %v\n", len(objects), path)

	// sort list of objects by timestamp
	if sortBy == "CreationTime" {
		sort.Sort(objectByCreationTime(objects))
	}

	var fileNames []string
	for _, obj := range objects {
		fileNames = append(fileNames, obj.Name)
	}

	if sortBy == "Natsort" {
		sort.Sort(natural.StringSlice(fileNames))
	}

	return fileNames, nil
}

func ReadCSVFile(client *storage.Client, ctx context.Context, bucketName, fileName string) ([][]string, error) {
	log.Debugf("Loading csv file gs://%v/%v", bucketName, fileName)

	// Get Object reader
	objectReader, err := GetObjectReader(client, ctx, bucketName, fileName)
	if err != nil {
		return nil, err
	}

	// Parse the CSV data.
	reader := csv.NewReader(objectReader)
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

	err = objectReader.Close()
	if err != nil {
		log.Errorf("Error closing file gs://%v/%v", bucketName, fileName)
	}

	return data, nil
}

func ReadFile(client *storage.Client, ctx context.Context, bucketName, fileName string) ([]byte, error) {
	path := fmt.Sprintf("gs://%v/%v", bucketName, fileName)
	log.Debugf("Reading file %v", path)

	// Get reader
	objectReader, err := GetObjectReader(client, ctx, bucketName, fileName)
	if err != nil {
		return nil, fmt.Errorf("failed to open GCS object: %v", err)
	}

	// Parse the CSV data.
	data := make([]byte, objectReader.Attrs.Size)
	numBytes, err := objectReader.Read(data)
	if err != nil {
		log.Errorf("Error reading file %v. Error is %v", path, err)
		return nil, err
	}
	log.Debugf("Loaded %v bytes from %v", numBytes, path)

	err = objectReader.Close()
	if err != nil {
		log.Errorf("Error closing file %v", path)
	}

	return data, nil
}

func GetObjectReader(client *storage.Client, ctx context.Context, bucketName, objectKey string) (*storage.Reader, error) {
	log.Debugf("Loading object reader for gs://%v/%v", bucketName, objectKey)

	// Open the bucket and object.
	bucket := client.Bucket(bucketName)
	obj := bucket.Object(objectKey)

	// Read the object from GCS.
	reader, err := obj.NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to open GCS object: %v", err)
	}
	return reader, nil
}

func GetObjectWriter(client *storage.Client, ctx context.Context, bucketName, objectKey string) (io.WriteCloser, error) {
	log.Debugf("Getting object writer for for gs://%v/%v", bucketName, objectKey)

	// Open the bucket and object.
	bucket := client.Bucket(bucketName)
	obj := bucket.Object(objectKey)

	// Read the object from GCS.
	writer := obj.NewWriter(ctx)
	return writer, nil
}

func WriteToFile(client *storage.Client, ctx context.Context, bucketName, objectKey string, data []byte) error {
	path := fmt.Sprintf("gs://%v/%v", bucketName, objectKey)

	log.Infof("Writing %v bytes to %v", len(data), path)
	// Get writer
	writer, err := GetObjectWriter(client, ctx, bucketName, objectKey)
	if err != nil {
		log.Errorf("Error getting writer for %v. Error is %v", path, err)
		return err
	}

	// Write the data
	numBytes, err := writer.Write(data)
	if err != nil {
		log.Errorf("Error writing data to %v. Error is %v", path, err)
		return err
	}

	// Close the file handle
	if err := writer.Close(); err != nil {
		log.Fatalf("Error closing file handle for %v. Error is: %v", path, err)
	}
	log.Infof("Wrote %v bytes to %v", numBytes, path)

	return nil
}

func WriteToCSVFile(client *storage.Client, ctx context.Context, bucketName, objectKey string, csvRecords [][]string) error {
	path := fmt.Sprintf("gs://%v/%v", bucketName, objectKey)

	log.Infof("Writing %v records to %v", len(csvRecords), path)
	// Get writer
	writer, err := GetObjectWriter(client, ctx, bucketName, objectKey)
	if err != nil {
		log.Errorf("Error getting writer for %v. Error is %v", path, err)
		return err
	}

	// Write the data
	csvWriter := csv.NewWriter(writer)
	err = csvWriter.WriteAll(csvRecords)
	csvWriter.Flush()
	if err != nil {
		log.Errorf("Error writing data to %v. Error is %v", path, err)
		return err
	}

	// Close the file handle
	if err := writer.Close(); err != nil {
		log.Fatalf("Error closing file handle for %v. Error is: %v", path, err)
	}
	log.Infof("Wrote %v records to %v", len(csvRecords), path)

	return nil
}
