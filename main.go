package main

import (
	"context"
	"log"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
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
	if localPath == "" {
		log.Fatalln("local path cannot be empty")
	}

	bucket = os.Args[2]
	if bucket == "" {
		log.Fatalln("bucket cannot be empty")
	}

	prefix = os.Args[3]
	if prefix == "" {
		log.Fatalln("prefix cannot be empty")
	}
}

func main() {
	walker := make(fileWalk)
	go func() {
		// Gather the files to upload by walking the path recursively
		if err := filepath.Walk(localPath, walker.Walk); err != nil {
			log.Fatalln("Walk failed:", err)
		}
		close(walker)
	}()

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatalln("Failed to load configuration:", err)
	}

	if bucket == "" {
		log.Fatalln("Bucket name cannot be empty")
	}

	// Upload the files to S3
	uploader := manager.NewUploader(s3.NewFromConfig(cfg))
	for path := range walker {
		rel, err := filepath.Rel(localPath, path)
		if err != nil {
			log.Println("Unable to get relative path:", path, err)
			continue
		}
		file, err := os.Open(path)
		if err != nil {
			log.Println("Failed opening file", path, err)
			continue
		}

		// Ensure the file is closed after the upload
		func() {
			defer file.Close()
			result, err := uploader.Upload(context.TODO(), &s3.PutObjectInput{
				Bucket: &bucket,
				Key:    aws.String(filepath.Join(prefix, rel)),
				Body:   file,
				// Ensure the storage class is Glacier
				StorageClass: types.StorageClassGlacier,
			})
			if err != nil {
				log.Println("Failed to upload", path, err)
				return
			}
			log.Println("Uploaded", path, result.Location)
		}()
	}
}

type fileWalk chan string

func (f fileWalk) Walk(path string, info os.FileInfo, err error) error {

	// Exclude specific macOS and external hard drive filenames
	excludedFiles := []string{
		".DS_Store",
		"._*",
		"$RECYCLE.BIN/*",
		"$RECYCLE.BIN/**",
		"**/._*",
		".LaCie/*",
		".Spotlight-V100/*",
		".Trashes/*",
		"*.inf",
		"*.ico",
		"*.icns",
		"*.apdisk",
	}

	for _, pattern := range excludedFiles {
		matches, globErr := filepath.Glob(pattern)
		if globErr != nil {
			return globErr
		}

		for _, match := range matches {
			removeErr := os.Remove(match)
			if removeErr != nil {
				return removeErr
			}
		}
	}

	if !info.IsDir() {
		f <- path
	}

	// Get list of local files
	localFiles := make(map[string]bool)
	walkErr := filepath.Walk(".", func(path string, info os.FileInfo, innerErr error) error {
		if !info.IsDir() {
			localFiles[path] = true
		}
		return nil
	})
	if walkErr != nil {
		return err
	}

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatalln("Failed to load configuration:", err)
	}

	s3Client := s3.NewFromConfig(cfg)

	// Get list of objects in S3 bucket
	objects, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		return err
	}

	// Delete objects in S3 bucket that are no longer present locally
	for _, obj := range objects.Contents {
		key := *obj.Key
		if _, ok := localFiles[key]; !ok {
			_, err := s3Client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			})
			if err != nil {
				log.Printf("Error deleting object %s: %v", key, err)
			} else {
				log.Printf("Deleted object %s", key)
			}
		}
	}

	return err
}
