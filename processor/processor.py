import os
import json
import boto3
import ffmpeg
import tempfile
import logging
from datetime import UTC, datetime
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# aws
endpoint_url = os.getenv('ENDPOINT_URL', None)
s3 = boto3.client('s3', endpoint_url=endpoint_url)
sqs = boto3.client('sqs', endpoint_url=endpoint_url)
dynamodb = boto3.resource('dynamodb', endpoint_url=endpoint_url)
table = dynamodb.Table(os.getenv('DYNAMODB_TABLE', 'audio-processing-status'))

# s3
UPLOAD_BUCKET = os.getenv('UPLOAD_BUCKET', 'audio-uploads')
PROCESSED_BUCKET = os.getenv('PROCESSED_BUCKET', 'processed-audio')
QUEUE_NAME = 'audio-processing-queue'

logger.info(f"Configuration: UPLOAD_BUCKET={UPLOAD_BUCKET}, PROCESSED_BUCKET={PROCESSED_BUCKET}, QUEUE_NAME={QUEUE_NAME}")

def get_queue_url():
    """Get the SQS queue URL, creating the queue if it doesn't exist"""
    try:
        # Try to get the queue URL first
        logger.info(f"Attempting to get queue URL for queue: {QUEUE_NAME}")
        response = sqs.get_queue_url(QueueName=QUEUE_NAME)
        queue_url = response['QueueUrl']
        logger.info(f"Got queue URL: {queue_url}")
        return queue_url
    except ClientError as e:
        if e.response['Error']['Code'] == 'AWS.SimpleQueueService.NonExistentQueue':
            # Queue doesn't exist, create it
            logger.info(f"Queue {QUEUE_NAME} doesn't exist, creating it...")
            response = sqs.create_queue(
                QueueName=QUEUE_NAME,
                Attributes={
                    'VisibilityTimeout': '300',
                    'MessageRetentionPeriod': '86400',
                    'DelaySeconds': '0',
                    'ReceiveMessageWaitTimeSeconds': '20'
                }
            )
            queue_url = response['QueueUrl']
            logger.info(f"Created queue with URL: {queue_url}")
            return queue_url
        logger.error(f"Error getting/creating queue: {str(e)}")
        raise

def update_status(upload_id, status, progress=None, error=None):
    """Update the processing status in DynamoDB"""
    try:
        logger.info(f"Updating status for {upload_id}: {status} (progress: {progress}, error: {error})")
        item = {
            'upload_id': upload_id,
            'status': status,
            'updated_at': datetime.now(UTC).isoformat()
        }
        if progress is not None:
            item['progress'] = progress
        if error is not None:
            item['error'] = error
            
        table.put_item(Item=item)
        logger.info(f"Status updated successfully for {upload_id}")
    except Exception as e:
        logger.error(f"Error updating status for {upload_id}: {str(e)}")

def process_audio(input_path, output_path, upload_id):
    """Process audio file:
    - 256 kbps bitrate
    - 48 kHz sample rate
    - Stereo (2-channel)
    - AAC codec, .m4a container
    """
    try:
        logger.info(f"Starting audio processing for {upload_id}")
        # Get input file duration
        probe = ffmpeg.probe(input_path)
        duration = float(probe['format']['duration'])
        logger.info(f"Input file duration: {duration} seconds")
        
        # Update status to processing
        update_status(upload_id, 'processing', progress=0)
        
        # Process the audio with progress tracking
        stream = ffmpeg.input(input_path)
        stream = ffmpeg.output(
            stream,
            output_path,
            acodec='aac',
            audio_bitrate='256k',
            ar='48000',
            ac=2,
            # format='ipod',  # This ensures .m4a container
            loglevel='info',
            y=None  # Force overwrite output files
        )
        
        logger.info("Starting FFmpeg processing")
        
        process = ffmpeg.run_async(stream, pipe_stdout=True, pipe_stderr=True)
        while process.poll() is None:
            try:
                stderr = process.stderr.read1().decode()
                if stderr:
                    logger.info(f"FFmpeg output: {stderr}")
                if 'time=' in stderr:
                    time_str = stderr.split('time=')[1].split()[0]
                    hours, minutes, seconds = map(float, time_str.split(':'))
                    current_time = hours * 3600 + minutes * 60 + seconds
                    progress = min(int((current_time / duration) * 100), 99)
                    update_status(upload_id, 'processing', progress=progress)
            except Exception as e:
                logger.warning(f"Error reading FFmpeg progress: {str(e)}")
        
        _, stderr = process.communicate()
        if stderr:
            logger.info(f"Final FFmpeg output: {stderr.decode()}")
        
        if process.returncode == 0:
            logger.info(f"Audio processing completed successfully for {upload_id}")
            update_status(upload_id, 'completed', progress=100)
            return True
        else:
            error_msg = f'FFmpeg processing failed with return code {process.returncode}'
            if stderr:
                error_msg += f': {stderr.decode()}'
            logger.error(f"Audio processing failed for {upload_id}: {error_msg}")
            update_status(upload_id, 'failed', error=error_msg)
            return False
            
    except ffmpeg.Error as e:
        error_msg = e.stderr.decode() if e.stderr else str(e)
        logger.error(f"FFmpeg error processing {upload_id}: {error_msg}")
        update_status(upload_id, 'failed', error=error_msg)
        return False
    except Exception as e:
        logger.error(f"Unexpected error processing {upload_id}: {str(e)}")
        update_status(upload_id, 'failed', error=str(e))
        return False

def process_message(message):
    try:
        logger.info(f"Processing message: {message['MessageId']}")
        body = json.loads(message['Body'])
        upload_id = body['upload_id']
        s3_key = body['s3_key']
        
        logger.info(f"Processing upload {upload_id} from {s3_key}")

        update_status(upload_id, 'downloading', progress=0)
        
        # temp files
        with tempfile.NamedTemporaryFile(suffix='.tmp', delete=False) as input_file, \
             tempfile.NamedTemporaryFile(suffix='.m4a', delete=False) as output_file:
            
            logger.info(f"Downloading file from S3: {UPLOAD_BUCKET}/{s3_key}")
            s3.download_fileobj(UPLOAD_BUCKET, s3_key, input_file)
            input_file.flush()
            
            # Process the audio
            if process_audio(input_file.name, output_file.name, upload_id):
                processed_key = f"{upload_id}/processed.m4a"
                logger.info(f"Uploading processed file to {PROCESSED_BUCKET}/{processed_key}")
                s3.upload_file(
                    output_file.name,
                    PROCESSED_BUCKET,
                    processed_key,
                    ExtraArgs={'ContentType': 'audio/mp4'}
                )
                logger.info(f"Successfully processed and uploaded file for {upload_id}")
                return True
            
        return False
        
    except Exception as e:
        logger.error(f"Error processing message: {str(e)}")
        update_status(upload_id, 'failed', error=str(e))
        return False
    finally:
        # clean up
        try:
            os.unlink(input_file.name)
            os.unlink(output_file.name)
        except Exception as e:
            logger.warning(f"Error cleaning up temporary files: {str(e)}")

def main():
    logger.info("Starting audio processor...")
    
    while True:
        try:
            queue_url = get_queue_url()
            logger.info(f"Using queue URL: {queue_url}")
            
            logger.info("Waiting for messages...")
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20
            )
            
            if 'Messages' in response:
                for message in response['Messages']:
                    logger.info(f"Received message: {message['MessageId']}")
                    if process_message(message):
                        logger.info(f"Deleting processed message: {message['MessageId']}")
                        sqs.delete_message(
                            QueueUrl=queue_url,
                            ReceiptHandle=message['ReceiptHandle']
                        )
                    else:
                        # If processing failed, make message visible again after delay
                        # OPTIONAL: retry processing after delay, backoff
                        logger.info(f"Making failed message visible again: {message['MessageId']}")
                        sqs.change_message_visibility(
                            QueueUrl=queue_url,
                            ReceiptHandle=message['ReceiptHandle'],
                            VisibilityTimeout=300  # 5 minutes
                        )
            else:
                logger.debug("No messages received")
                        
        except Exception as e:
            logger.error(f"Error in main loop: {str(e)}")
            continue

if __name__ == '__main__':
    main() 