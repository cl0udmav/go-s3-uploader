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
)

var (
	localDir   string
	bucket     string
	prefix     string
	excludes   []string
	s3Client   *s3.Client
	uploader   *manager.Uploader
	localFiles map[string]bool
)

func main() {
	initVars()
	initS3Client()
	walkLocalDir()
	uploadFiles()
	deleteRemovedFiles()
}

func initVars() {
	if len(os.Args) != 4 {
		log.Fatalln("Usage:", os.Args[0], "<local dir> <bucket> <prefix>")
	}

	localDir = os.Args[1]
	if localDir == "" {
		log.Fatalln("local dir cannot be empty")
	}

	bucket = os.Args[2]
	if bucket == "" {
		log.Fatalln("bucket cannot be empty")
	}

	prefix = os.Args[3]
	if prefix == "" {
		log.Fatalln("prefix cannot be empty")
	}

	excludes = []string{
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
}

func initS3Client() {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatalln("Failed to load configuration:", err)
	}

	s3Client = s3.NewFromConfig(cfg)
	uploader = manager.NewUploader(s3Client)
}

func walkLocalDir() {
	localFiles = make(map[string]bool)
	walkErr := filepath.Walk(localDir, func(path string, info os.FileInfo, innerErr error) error {
		if innerErr != nil {
			return innerErr
		}
		if info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(localDir, path)
		if err != nil {
			log.Println("Unable to get relative path:", path, err)
			return nil
		}
		for _, exclude := range excludes {
			if matched, err := filepath.Match(exclude, rel); err != nil || matched {
				log.Printf("Excluding file: %s\n", rel)
				return nil
			}
		}
		localFiles[path] = true
		return nil
	})
	if walkErr != nil {
		log.Fatalln("Walk failed:", walkErr)
	}
}

func uploadFiles() {
	for path := range localFiles {
		rel, err := filepath.Rel(localDir, path)
		if err != nil {
			log.Println("Unable to get relative path:", path, err)
			continue
		}

		// Check if the object already exists in the S3 bucket
		obj, err := s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(prefix + "/" + rel),
		})
		if err != nil {
			log.Println("Skipping update of non-existent file:", path)
		} else {
			// Check the object's current storage class
			currentStorageClass := obj.StorageClass
			if currentStorageClass == "" {
				currentStorageClass = "STANDARD" // default storage class
			}

			// If the object's storage class is not already GLACIER, update it
			if currentStorageClass != "GLACIER" {
				log.Printf("Updating storage class of %q from %q to GLACIER\n", path, currentStorageClass)
			}
		}

		// Upload the file to S3
		f, err := os.Open(path)
		if err != nil {
			log.Println("Error opening file:", err)
			continue
		}
		defer f.Close()

		_, err = uploader.Upload(context.Background(), &s3.PutObjectInput{
			Bucket:       aws.String(bucket),
			Key:          aws.String(prefix + "/" + rel),
			Body:         f,
			StorageClass: "GLACIER",
		})
		if err != nil {
			log.Println("Error uploading file:", err)
			continue
		}

		log.Println("Uploaded file:", path)
		uploadFile(path, rel)
	}
}

func uploadFile(localPath string, rel string) {
	file, err := os.Open(localPath)
	if err != nil {
		log.Println("Unable to open file:", localPath, err)
		return
	}
	defer file.Close()

	_, err = uploader.Upload(context.Background(), &s3.PutObjectInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(prefix + "/" + rel),
		Body:         file,
		StorageClass: "GLACIER",
	})
	if err != nil {
		log.Println("Unable to upload:", localPath, err)
	} else {
		log.Println("Uploaded:", localPath)
	}
}

func deleteRemovedFiles() {
	// Get list of objects in S3 bucket
	objects, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		log.Fatalln("Failed to list objects in S3 bucket:", err)
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
}
