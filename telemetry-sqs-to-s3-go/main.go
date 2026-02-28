package main

import (
	"context"
	"log"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
)

// var s3Client *s3.Client
// var bucketName string

// func init() {
// 	bucketName = os.Getenv("TELEMETRY_TEMPORARY_BUCKET_NAME")
// 	if bucketName == "" {
// 		log.Fatal("TELEMETRY_TEMPORARY_BUCKET_NAME is not set")
// 	}
// 	cfg, err := config.LoadDefaultConfig(context.Background())
// 	if err != nil {
// 		log.Fatalf("load AWS config: %v", err)
// 	}
// 	s3Client = s3.NewFromConfig(cfg)
// }

func handler(ctx context.Context, sqsEvent events.SQSEvent) error {
	log.Printf("process message %v", sqsEvent)
	return nil
}

func main() {
	lambda.Start(handler)
}
