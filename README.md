# Freakyfit Media Extractor

A dedicated, standalone microservice and AWS Lambda worker for:
1. **Downloading video and images** from Twitter / X, YouTube, and Instagram (via `yt-dlp` and API extractors).
2. **Uploading media to AWS S3** with Range header streaming, 206 Partial Content, and pre-signed URL generation.
3. **Uploading videos directly to Google Gemini File API** to generate `geminiFileUri` for zero-download LLM extraction.
4. **Dispatching results directly to `freakyfit-backend` AWS SQS** to trigger fast, video-free workout extraction.

---

## Architecture Overview

```
[Mobile App / User]
       │
       ▼ (1) POST /api/generate-workout
[freakyfit-backend] ──> pushes extraction job to SQS
       │
       ▼
┌─────────────────────────────────────────────────────────────┐
│ FREAKYFIT MEDIA EXTRACTOR (This Project)                    │
│                                                             │
│ - Downloads video/image from Twitter, YouTube, Instagram   │
│ - Uploads video directly to Gemini File API                 │
│ - Uploads media to AWS S3 (e.g. temp/workouts/...)         │
│ - Pushes message directly to freakyfit-backend SQS:         │
│   {                                                         │
│     "action": "llm_workout_extraction",                     │
│     "jobId": "...",                                         │
│     "geminiFileUri": "https://generativelanguage.../files", │
│     "geminiFileName": "files/...",                          │
│     "s3Key": "temp/workouts/...",                           │
│     "deleteFromS3": true                                    │
│   }                                                         │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼ SQS
┌─────────────────────────────────────────────────────────────┐
│ FREAKYFIT BACKEND (LLM Worker)                              │
│                                                             │
│ - 🚀 ZERO VIDEO DOWNLOAD (never buffers any video bytes)    │
│ - Calls Gemini directly with `geminiFileUri`                │
│ - Inserts workout into Supabase DB                          │
│ - Deletes temporary video from S3 and Gemini                │
│ - Sends Expo Push Notification to User                      │
└─────────────────────────────────────────────────────────────┘
```

---

## REST API Endpoints

### 1. Download Media & Save to S3
- **Route**: `POST /api/media/download`
- **Request Body**:
  ```json
  {
    "url": "https://x.com/username/status/1234567890",
    "type": "video",       // "video" | "image" | "auto" (default: "auto")
    "prefix": "media",     // optional S3 prefix (default: "media")
    "bucket": "my-bucket", // optional custom S3 bucket
    "deleteFromS3": false, // set true for temporary workout extraction
    "uploadToGemini": true,// uploads to Gemini File API if true
    "jobId": "job-uuid",   // optional: if provided, notifies BACKEND_SQS_QUEUE_URL
    "userId": "user-uuid"
  }
  ```
- **Response (`201 Created`)**:
  ```json
  {
    "success": true,
    "platform": "twitter",
    "mediaType": "video",
    "key": "media/media-1795042913386-02blw8.mp4",
    "bucket": "freakyfit-media",
    "s3Url": "https://freakyfit-media.s3.ap-southeast-2.amazonaws.com/...",
    "contentType": "video/mp4",
    "fileSize": 1542039,
    "geminiFileUri": "https://generativelanguage.googleapis.com/v1beta/files/xyz",
    "geminiFileName": "files/xyz",
    "deleteFromS3": false
  }
  ```

---

### 2. Stream / Load Media from S3
- **Route**: `GET /api/media/:key(*)` or `GET /api/media?key=<key>`
- **Features**:
  - Supports HTTP `Range` headers (responds with `206 Partial Content`) for video seeking.
  - Supports `?download=true` for direct attachment download.
  - Supports `?redirect=true` for 302 redirect to temporary pre-signed URL.

---

### 3. Get Pre-Signed S3 URL
- **Route**: `GET /api/media/url/:key(*)` or `GET /api/media/url?key=<key>`
- **Response**:
  ```json
  {
    "success": true,
    "key": "media/video.mp4",
    "bucket": "freakyfit-media",
    "presignedUrl": "https://freakyfit-media.s3.amazonaws.com/...",
    "expiresIn": 3600
  }
  ```

---

### 4. Delete Media from S3
- **Route**: `DELETE /api/media/:key(*)` or `DELETE /api/media?key=<key>`
- **Response**:
  ```json
  {
    "success": true,
    "message": "Object deleted from S3.",
    "key": "temp/workouts/media-123.mp4"
  }
  ```

---

## Environment Variables

Copy `.env.example` to `.env`:
```env
PORT=3001
AWS_REGION=ap-southeast-2
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_S3_BUCKET=freakyfit-media

# SQS Queue URLs
SQS_QUEUE_URL=https://sqs.ap-southeast-2.amazonaws.com/.../video-extraction-queue
BACKEND_SQS_QUEUE_URL=https://sqs.ap-southeast-2.amazonaws.com/.../freakyfit-backend-queue

GEMINI_API_KEY=your_gemini_key
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_SERVICE_ROLE_KEY=your_key
```

---

## Running Locally

```bash
npm install
npm test      # Runs all 7 integration & unit test suites
npm run dev   # Starts local Express server on port 3001
```

---

## AWS Lambda & Docker Deployment

Built to run on AWS Lambda via container images:
```bash
docker build -f Dockerfile.lambda -t freakyfit-media-extractor .
```
- **Handler**: `worker.handler`
- **Timeout**: Set to `300` seconds (5 minutes)
- **Memory**: Set to `2048` MB
