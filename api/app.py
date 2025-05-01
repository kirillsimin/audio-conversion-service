from fastapi import FastAPI, UploadFile, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import boto3
import os
import uuid
from typing import Optional, Set
from datetime import datetime, UTC
import json
from botocore.exceptions import ClientError
from enum import Enum

class ProcessingState(str, Enum):
    """Valid processing states"""
    UPLOADING = 'uploading'
    QUEUED = 'queued'
    PROCESSING = 'processing'
    COMPLETED = 'completed'
    FAILED = 'failed'

class Config:
    """Application configuration"""
    # AWS/LocalStack configuration
    ENDPOINT_URL = os.getenv('ENDPOINT_URL')
    S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL', ENDPOINT_URL)  # Separate S3 endpoint for client access
    
    # Buckets and queues
    UPLOAD_BUCKET = os.getenv('UPLOAD_BUCKET', 'audio-uploads')
    PROCESSED_BUCKET = os.getenv('PROCESSED_BUCKET', 'processed-audio')
    QUEUE_NAME = os.getenv('QUEUE_NAME', 'audio-processing-queue')
    
    # Other settings
    ALLOWED_EXTENSIONS: Set[str] = {'wav', 'aac', 'm4a', 'mp3'}
    MAX_FILE_SIZE = 200 * 1024 * 1024  # 200MB
    ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*").split(",")
    DYNAMODB_TABLE = os.getenv('DYNAMODB_TABLE', 'audio-processing-status')

app = FastAPI(
    title="Audio Processing API",
    description="API for uploading and processing audio files",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=Config.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# AWS clients
s3 = boto3.client('s3', endpoint_url=Config.ENDPOINT_URL)
sqs = boto3.client('sqs', endpoint_url=Config.ENDPOINT_URL)
dynamodb = boto3.resource('dynamodb', endpoint_url=Config.ENDPOINT_URL)
table = dynamodb.Table(Config.DYNAMODB_TABLE)

def get_queue_url():
    """Get the SQS queue URL, creating the queue if it doesn't exist"""
    try:
        # Try to get the queue URL first
        response = sqs.get_queue_url(QueueName=Config.QUEUE_NAME)
        return response['QueueUrl']
    except ClientError as e:
        if e.response['Error']['Code'] == 'AWS.SimpleQueueService.NonExistentQueue':
            # Queue doesn't exist, create it
            response = sqs.create_queue(
                QueueName=Config.QUEUE_NAME,
                Attributes={
                    'VisibilityTimeout': '300',
                    'MessageRetentionPeriod': '86400',
                    'DelaySeconds': '0',
                    'ReceiveMessageWaitTimeSeconds': '20'
                }
            )
            return response['QueueUrl']
        raise

class StatusResponse(BaseModel):
    """Base response model for status endpoints"""
    upload_id: str
    status: ProcessingState
    progress: Optional[int] = None
    error: Optional[str] = None
    updated_at: Optional[str] = None

class UploadResponse(StatusResponse):
    """Response model for upload endpoint"""
    message: str

class ProcessingStatus(StatusResponse):
    """Response model for status endpoint"""
    processed_url: Optional[str] = None

def allowed_file(filename: str) -> bool:
    """Check if file extension is allowed"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in Config.ALLOWED_EXTENSIONS

def update_status(upload_id: str, status: ProcessingState, progress: int = 0, error: Optional[str] = None) -> None:
    """Update the processing status in DynamoDB"""
    table.put_item(Item={
        'upload_id': upload_id,
        'status': status,
        'progress': progress,
        'error': error,
        'updated_at': datetime.now(UTC).isoformat()
    })

@app.post("/upload", response_model=UploadResponse, tags=["Audio"])
async def upload_audio(file: UploadFile, background_tasks: BackgroundTasks):
    """
    Upload an audio file for processing.
    
    - **file**: Audio file to upload (supported formats: wav, aac, m4a, mp3)
    - **max_size**: Maximum file size: 200MB
    """
    if not allowed_file(file.filename):
        raise HTTPException(status_code=400, detail="File type not allowed")
    
    # check file size
    contents = await file.read()
    if len(contents) > Config.MAX_FILE_SIZE:
        raise HTTPException(
            status_code=413,
            detail=f"File size exceeds the 200MB limit. Current size: {len(contents) / (1024 * 1024):.2f}MB"
        )
    
    # unique id
    upload_id = str(uuid.uuid4())
    filename = file.filename
    s3_key = f"{upload_id}/{filename}"
    
    try:
        update_status(upload_id, ProcessingState.UPLOADING)
        
        s3.put_object(
            Bucket=Config.UPLOAD_BUCKET,
            Key=s3_key,
            Body=contents
        )
        
        update_status(upload_id, ProcessingState.QUEUED)
        
        queue_url = get_queue_url()
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps({
                'upload_id': upload_id,
                'original_filename': filename,
                's3_key': s3_key
            })
        )
        
        return UploadResponse(
            upload_id=upload_id,
            status=ProcessingState.QUEUED,
            message='File uploaded successfully and queued for processing'
        )
        
    except Exception as e:
        update_status(upload_id, ProcessingState.FAILED, error=str(e))
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/status/{upload_id}", response_model=ProcessingStatus, tags=["Audio"])
async def get_status(upload_id: str):
    """
    Get the current status of an uploaded audio file.
    
    - **upload_id**: The ID of the upload to check
    """
    try:
        response = table.get_item(Key={'upload_id': upload_id})
        
        if 'Item' not in response:
            raise HTTPException(status_code=404, detail="Upload not found")
            
        item = response['Item']
        status = item['status']
        
        # If processing is complete, include the processed file URL
        if status == ProcessingState.COMPLETED:
            processed_key = f"{upload_id}/processed.m4a"
            # Use configured S3 endpoint URL if available, otherwise use AWS S3 URL
            if Config.S3_ENDPOINT_URL:
                processed_url = f"{Config.S3_ENDPOINT_URL}/{Config.PROCESSED_BUCKET}/{processed_key}"
            else:
                processed_url = f"https://{Config.PROCESSED_BUCKET}.s3.amazonaws.com/{processed_key}"
            return ProcessingStatus(
                upload_id=upload_id,
                status=status,
                progress=item.get('progress', 100),
                processed_url=processed_url,
                updated_at=item.get('updated_at')
            )
            
        return ProcessingStatus(
            upload_id=upload_id,
            status=status,
            progress=item.get('progress', 0),
            error=item.get('error'),
            updated_at=item.get('updated_at')
        )
                
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/download/{upload_id}", tags=["Audio"])
async def download(upload_id: str):
    """
    Download the processed file.
    
    - **upload_id**: File ID to be downloaded
    """
    try:
        response = table.get_item(Key={'upload_id': upload_id})
        
        if 'Item' not in response:
            raise HTTPException(status_code=404, detail="Upload not found")
            
        item = response['Item']
        if item['status'] != 'completed':
            raise HTTPException(
                status_code=400, 
                detail=f"File is not ready for download. Current status: {item['status']}"
            )

        processed_key = f"{upload_id}/processed.m4a"
        try:
            response = s3.get_object(Bucket=Config.PROCESSED_BUCKET, Key=processed_key)
        except Exception as e:
            raise HTTPException(status_code=404, detail="Processed file not found")

        return StreamingResponse(
            response['Body'],
            media_type='audio/mp4',
            headers={
                'Content-Disposition': f'attachment; filename="processed_{upload_id}.m4a"'
            }
        )
                
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000) 