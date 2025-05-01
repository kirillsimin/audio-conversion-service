#!/bin/bash

# Load environment variables from .env file
if [ -f .env ]; then
    set -a
    source .env
    set +a
else
    echo "Error: .env file not found"
    exit 1
fi

# Start Docker Compose services
echo "Starting Docker Compose services..."
docker compose up -d

# Wait for LocalStack to be ready
echo "Setting up LocalStack..."
while ! curl -s http://localhost:4566/_localstack/health | grep -q '"s3": "\(available\|running\)"'; do
    echo "Waiting for LocalStack..."
    sleep 2
done

# Create S3 buckets
echo "Creating S3 buckets..."
aws --endpoint-url=http://localhost:4566 s3api create-bucket --bucket ${UPLOAD_BUCKET}
aws --endpoint-url=http://localhost:4566 s3api create-bucket --bucket ${PROCESSED_BUCKET}

# Make buckets public
echo "Making buckets public..."
aws --endpoint-url=http://localhost:4566 s3api put-bucket-acl --bucket ${UPLOAD_BUCKET} --acl public-read-write
aws --endpoint-url=http://localhost:4566 s3api put-bucket-acl --bucket ${PROCESSED_BUCKET} --acl public-read-write

# Create bucket policies to allow public access
echo "Setting bucket policies..."
aws --endpoint-url=http://localhost:4566 s3api put-bucket-policy --bucket ${UPLOAD_BUCKET} --policy '{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "PublicReadWrite",
            "Effect": "Allow",
            "Principal": "*",
            "Action": ["s3:GetObject", "s3:PutObject"],
            "Resource": ["arn:aws:s3:::'${UPLOAD_BUCKET}'/*"]
        }
    ]
}'

aws --endpoint-url=http://localhost:4566 s3api put-bucket-policy --bucket ${PROCESSED_BUCKET} --policy '{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "PublicReadWrite",
            "Effect": "Allow",
            "Principal": "*",
            "Action": ["s3:GetObject", "s3:PutObject"],
            "Resource": ["arn:aws:s3:::'${PROCESSED_BUCKET}'/*"]
        }
    ]
}'

# Create SQS queue
echo "Creating SQS queue..."
QUEUE_URL=$(aws --endpoint-url=http://localhost:4566 sqs create-queue --queue-name ${QUEUE_NAME} --query 'QueueUrl' --output text)
echo "Queue URL: $QUEUE_URL"

# Create DynamoDB table
echo "Creating DynamoDB table..."
aws --endpoint-url=http://localhost:4566 dynamodb create-table \
    --table-name ${DYNAMODB_TABLE} \
    --attribute-definitions \
        AttributeName=upload_id,AttributeType=S \
    --key-schema \
        AttributeName=upload_id,KeyType=HASH \
    --provisioned-throughput \
        ReadCapacityUnits=5,WriteCapacityUnits=5

echo "LocalStack resources initialized successfully!" 