import pytest
from fastapi.testclient import TestClient
from app import app, ProcessingState, Config
import boto3
import os
import json
from moto import mock_aws
import io
import time
from fastapi import UploadFile
from typing import Optional

# Create test client
client = TestClient(app)

# Test data
TEST_AUDIO_CONTENT = b"fake audio content"
TEST_UPLOAD_ID = "test-upload-id"
TEST_FILENAME = "test.mp3"

class MockUploadFile(UploadFile):
    """Mock UploadFile that allows setting content_type to None"""
    def __init__(self, file: io.BytesIO, filename: str, content_type: Optional[str] = None):
        super().__init__(file=file, filename=filename)
        self._content_type = content_type

    @property
    def content_type(self) -> Optional[str]:
        return self._content_type

@pytest.fixture(autouse=True)
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    # Set LocalStack endpoint for testing
    os.environ["ENDPOINT_URL"] = "http://localhost:4566"
    os.environ["S3_ENDPOINT_URL"] = "http://localhost:4566"
    # Set bucket names
    os.environ["UPLOAD_BUCKET"] = "audio-uploads"
    os.environ["PROCESSED_BUCKET"] = "processed-audio"
    os.environ["DYNAMODB_TABLE"] = "audio-processing-status"
    os.environ["QUEUE_NAME"] = "audio-processing-queue"

@pytest.fixture
def aws_mock(aws_credentials):
    """Create mock AWS services."""
    with mock_aws():
        # Create S3 client and buckets
        s3 = boto3.client("s3", region_name="us-east-1", endpoint_url=Config.ENDPOINT_URL)
        s3.create_bucket(Bucket=Config.UPLOAD_BUCKET)
        s3.create_bucket(Bucket=Config.PROCESSED_BUCKET)
        
        # Create SQS client and queue
        sqs = boto3.client("sqs", region_name="us-east-1", endpoint_url=Config.ENDPOINT_URL)
        sqs.create_queue(QueueName=Config.QUEUE_NAME)
        
        # Create DynamoDB client and table
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1", endpoint_url=Config.ENDPOINT_URL)
        table = dynamodb.create_table(
            TableName=Config.DYNAMODB_TABLE,
            KeySchema=[{"AttributeName": "upload_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "upload_id", "AttributeType": "S"}],
            ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
        )
        
        # Wait for table to be created
        table.meta.client.get_waiter('table_exists').wait(TableName=Config.DYNAMODB_TABLE)
        
        # Update app's AWS clients to use mock services
        app.state.s3 = s3
        app.state.sqs = sqs
        app.state.dynamodb = dynamodb
        app.state.table = table
        
        yield {
            "s3": s3,
            "sqs": sqs,
            "dynamodb": dynamodb
        }

@pytest.mark.parametrize("mime_type,filename,should_pass", [
    # Valid MIME types with valid extensions
    ('audio/mpeg', 'test.mp3', True),
    ('audio/mp3', 'test.mp3', True),
    ('audio/wav', 'test.wav', True),
    ('audio/x-wav', 'test.wav', True),
    ('audio/aac', 'test.aac', True),
    ('audio/mp4', 'test.m4a', True),
    
    # Invalid MIME types with valid extensions (should fail at MIME type check)
    ('image/jpeg', 'test.mp3', False),
    ('text/plain', 'test.mp3', False),
    ('application/pdf', 'test.mp3', False),
    ('video/mp4', 'test.mp3', False),
    ('audio/ogg', 'test.mp3', False),  # Not in allowed types
])
def test_mime_type_validation(aws_mock, mime_type: str, filename: str, should_pass: bool):
    """Test MIME type validation for file uploads"""
    response = client.post(
        "/upload",
        files={"file": (filename, TEST_AUDIO_CONTENT, mime_type)}
    )
    
    if should_pass:
        assert response.status_code == 200
        assert "upload_id" in response.json()
    else:
        assert response.status_code == 400
        assert "File MIME type not allowed" in response.json()["detail"]

def test_mime_type_with_invalid_extension(aws_mock):
    """Test MIME type validation when file extension doesn't match content type"""
    response = client.post(
        "/upload",
        files={"file": ("test.txt", TEST_AUDIO_CONTENT, "audio/mpeg")}
    )
    
    # Should fail because extension doesn't match allowed types
    assert response.status_code == 400
    assert "File type not allowed" in response.json()["detail"]

def test_mime_type_with_missing_content_type(aws_mock):
    """Test upload with missing content type"""
    # Create a file upload with empty content type
    files = {
        "file": (
            "test.mp3",
            TEST_AUDIO_CONTENT,
            ""  # Empty content type
        )
    }
    response = client.post("/upload", files=files)
    
    # Should fail because content type is required
    assert response.status_code == 400
    assert "File MIME type is required" in response.json()["detail"]

def test_upload_endpoint_invalid_file_type(aws_mock):
    """Test upload endpoint with invalid file type."""
    response = client.post(
        "/upload",
        files={"file": ("test.txt", TEST_AUDIO_CONTENT, "text/plain")}
    )
    assert response.status_code == 400
    assert response.json()["detail"] == "File type not allowed"

def test_upload_endpoint_file_too_large(aws_mock):
    """Test upload endpoint with file exceeding size limit."""
    large_content = b"x" * (201 * 1024 * 1024)  # 201MB
    response = client.post(
        "/upload",
        files={"file": (TEST_FILENAME, large_content, "audio/mpeg")}
    )
    assert response.status_code == 413
    assert "File size exceeds the 200MB limit" in response.json()["detail"]

def test_upload_endpoint_success(aws_mock):
    """Test successful file upload."""
    response = client.post(
        "/upload",
        files={"file": (TEST_FILENAME, TEST_AUDIO_CONTENT, "audio/mpeg")}
    )
    assert response.status_code == 200
    data = response.json()
    assert "upload_id" in data
    assert data["status"] == ProcessingState.QUEUED
    assert data["message"] == "File uploaded successfully and queued for processing"

def test_status_endpoint_not_found(aws_mock):
    """Test status endpoint with non-existent upload ID."""
    response = client.get(f"/status/{TEST_UPLOAD_ID}")
    assert response.status_code == 404
    assert response.json()["detail"] == "Upload not found"

def test_status_endpoint_success(aws_mock):
    """Test status endpoint with existing upload."""
    # Create test status in DynamoDB
    table = aws_mock["dynamodb"].Table(Config.DYNAMODB_TABLE)
    table.put_item(
        Item={
            "upload_id": TEST_UPLOAD_ID,
            "status": ProcessingState.PROCESSING,
            "progress": 50,
            "updated_at": "2024-03-20T12:00:00Z"
        }
    )
    
    response = client.get(f"/status/{TEST_UPLOAD_ID}")
    assert response.status_code == 200
    data = response.json()
    assert data["upload_id"] == TEST_UPLOAD_ID
    assert data["status"] == ProcessingState.PROCESSING
    assert data["progress"] == 50

def test_download_endpoint_not_found(aws_mock):
    """Test download endpoint with non-existent upload ID."""
    response = client.get(f"/download/{TEST_UPLOAD_ID}")
    assert response.status_code == 404
    assert response.json()["detail"] == "Upload not found"

def test_download_endpoint_not_ready(aws_mock):
    """Test download endpoint when file is not ready."""
    # Create test status in DynamoDB
    table = aws_mock["dynamodb"].Table(Config.DYNAMODB_TABLE)
    table.put_item(
        Item={
            "upload_id": TEST_UPLOAD_ID,
            "status": ProcessingState.PROCESSING,
            "progress": 50,
            "updated_at": "2024-03-20T12:00:00Z"
        }
    )
    
    response = client.get(f"/download/{TEST_UPLOAD_ID}")
    assert response.status_code == 400
    assert "File is not ready for download" in response.json()["detail"]

def test_download_endpoint_success(aws_mock):
    """Test successful file download."""
    # Create test status in DynamoDB
    table = aws_mock["dynamodb"].Table(Config.DYNAMODB_TABLE)
    table.put_item(
        Item={
            "upload_id": TEST_UPLOAD_ID,
            "status": ProcessingState.COMPLETED,
            "progress": 100,
            "updated_at": "2024-03-20T12:00:00Z"
        }
    )
    
    # Upload test file to S3
    aws_mock["s3"].put_object(
        Bucket=Config.PROCESSED_BUCKET,
        Key=f"{TEST_UPLOAD_ID}/processed.m4a",
        Body=TEST_AUDIO_CONTENT
    )
    
    response = client.get(f"/download/{TEST_UPLOAD_ID}")
    assert response.status_code == 200
    assert response.headers["content-type"] == "audio/mp4"
    assert response.headers["content-disposition"] == f'attachment; filename="processed_{TEST_UPLOAD_ID}.m4a"'
    assert response.content == TEST_AUDIO_CONTENT 