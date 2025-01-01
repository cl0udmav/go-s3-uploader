# go-s3-uploader

## Usage

I made this to backup the many hours of cat videos on my external hard drive. Worth the S3 storage costs.

Run the script from the command line using positional arguments for local filesystem path, S3 bucket name, S3 path prefix.

```text
go run main.go ${localPath} ${bucketName} ${s3PathPrefix}
```

- Excludes specific macOS filenamess and external hard drive system files
- Deletes objects from bucket if no longer present in local filesystem

I used examples from this documentation and modified them for my needs:

- [AWS SDK for Go](https://aws.github.io/aws-sdk-go-v2/docs/sdk-utilities/s3/)
- Go [path/filepath](https://pkg.go.dev/path/filepath) package.
