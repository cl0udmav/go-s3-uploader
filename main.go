package main

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/aws"
)

var (
	localPath string
	bucket    string
	prefix    string
)

func init() {
	if len(os.Args) != 4 {
		log.Fatalln("Usage:", os.Args[0], "<local path> <bucket> <prefix>")
	}
	localPath = os.Args[1]
	bucket = os.Args[2]
	prefix = os.Args[3]
}

func main() {
	// Gather the files to upload by walking the path recursively
	var files []string
	if err := filepath.Walk(localPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		// Exclude directories
		if info.IsDir() {
			return nil
		}
		// Exclude filenames that start with "._"
		if strings.HasPrefix(info.Name(), "._") {
			return nil
		}
		files = append(files, path)
		return nil
	}); err != nil {
		log.Fatalln("Walk failed:", err)
	}

	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatalln("error:", err)
	}

	// Create S3 client
	client := s3.NewFromConfig(cfg)

	// Create S3 uploader
	uploader := manager.NewUploader(client)

	// List all objects in the bucket
	listObjectsOutput, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		log.Fatalf("Failed to list objects in bucket: %v", err)
	}

	// Store object keys in a map for quick lookup
	existingKeys := make(map[string]struct{})
	for _, object := range listObjectsOutput.Contents {
		existingKeys[*object.Key] = struct{}{}
	}

	// Create a worker pool
	const workerCount = 10
	fileChan := make(chan string)
	var wg sync.WaitGroup

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for path := range fileChan {
				rel, err := filepath.Rel(localPath, path)
				if err != nil {
					log.Println("Unable to get relative path:", path, err)
					continue
				}
				objectKey := filepath.Join(prefix, rel)
				// Check if the object key already exists in S3
				if _, ok := existingKeys[objectKey]; ok {
					log.Printf("Skipped upload for %s - Already exists in S3\n", path)
					continue
				}

				// Open the file
				file, err := os.Open(path)
				if err != nil {
					log.Println("Failed opening file", path, err)
					continue
				}

				// Upload the file to S3
				_, err = uploader.Upload(context.TODO(), &s3.PutObjectInput{
					Bucket: &bucket,
					Key:    &objectKey,
					Body:   file,
				})
				file.Close() // Ensure the file is closed after the upload
				if err != nil {
					log.Println("Failed to upload", path, err)
				} else {
					log.Println("Uploaded", path, "to", objectKey)
				}
			}
		}()
	}

	// Send files to workers
	for _, path := range files {
		fileChan <- path
	}
	close(fileChan)
	wg.Wait()
}
