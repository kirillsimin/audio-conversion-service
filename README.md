# Audio Conversion Service

This service:
- Accepts audio file uploads (up to 200MB, common formats: .wav, .aac, .m4a, .mp3)
- Converts audio files to a standardized format:
    - 256 kbps bitrate
    - 48 kHz sample rate
    - Stereo (2-channel)
    - AAC codec, .m4a container
- Returns a URL where the processed file can be accessed
- Allows progress tracking during upload and/or processing

## Local Setup

1. Copy the example environment file:
```bash
cp .env-example .env
```

2. Start the local infrastructure:
```bash
./local-setup.sh
```

This will start:
- LocalStack (AWS emulator)
- API service
- Audio processor service

## Local Endpoints

- API docs: `http://localhost:5000/docs`
- LocalStack: `http://localhost:4566/_localstack/health`
- S3 Upload Bucket: `http://localhost:4566/audio-uploads`
- S3 Processed Bucket: `http://localhost:4566/processed-audio`
- SQS Queue: `audio-processing-queue`
- DynamoDB Table: `audio-processing`


## API Endpoints

### Upload Audio
```http
POST /upload
Content-Type: multipart/form-data

file: <audio_file>
```

### Get Processing Status
```http
GET /status/{upload_id}
```

### Download Processed Audio
```http
GET /download/{upload_id}
```
