# telemetry-sqs-to-s3-go

Go AWS Lambda that consumes messages from the telemetry SQS queue and writes each message body to S3 under `telemetry/YYYY/MM/DD/` in the temporary bucket.

## Requirements

- Go 1.21+
- Set `TELEMETRY_TEMPORARY_BUCKET_NAME` (provided by CDK when deployed)

## Build for Lambda (provided.al2023)

```bash
make
```

Produces `bootstrap.zip` with the `bootstrap` binary. The CDK stack builds this via Docker when you deploy.

## Local build

```bash
go build -o bootstrap .
```
