package main

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var (
	lastRuntime time.Time
	localPath   string
	bucket      string
	prefix      string
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
	// Create a local cache file to check
	// last script runtime date
	lastRuntimeFilePath := filepath.Join(localPath, "._S3UploaderLastRuntime")
	lastRuntimeFileInfo, err := os.Stat(lastRuntimeFilePath)
	if err == nil {
		lastRuntime = lastRuntimeFileInfo.ModTime()
	} else {
		lastRuntime = time.Time{}
	}

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
		log.Fatalln("error:", err)
	}

	// For each file found walking, upload it to Amazon S3
	uploader := manager.NewUploader(s3.NewFromConfig(cfg))
	for path := range walker {
		rel, err := filepath.Rel(localPath, path)
		if err != nil {
			log.Fatalln("Unable to get relative path:", path, err)
		}
		file, err := os.Open(path)
		if err != nil {
			log.Println("Failed opening file", path, err)
			continue
		}
		defer file.Close()
		result, err := uploader.Upload(context.TODO(), &s3.PutObjectInput{
			Bucket:       &bucket,
			Key:          aws.String(filepath.Join(prefix, rel)),
			Body:         file,
			StorageClass: "GLACIER",
		})
		if err != nil {
			log.Fatalln("Failed to upload", path, err)
		}
		log.Println("Uploaded", path, result.Location)
	}

	currentTimeBytes := []byte(time.Now().UTC().String())
	os.WriteFile(lastRuntimeFilePath, currentTimeBytes, 0755)
}

type fileWalk chan string

func (f fileWalk) Walk(path string, info os.FileInfo, err error) error {
	// Exclude directories
	if info.IsDir() {
		return nil
	}

	// Exclude filenames that start with "._"
	if strings.HasPrefix(info.Name(), "._") {
		return nil
	}

	// Exclude files that are already up-to-date in S3
	// We know they're up-to-date if the last script runtime
	// file hasn't been modified since the last script run
	if info.ModTime().Before(lastRuntime) {
		return nil
	}

	// Add path to fileWalk
	f <- path
	return nil
}
