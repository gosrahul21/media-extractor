import path from 'path';
import fs from 'fs';
import serverlessExpress from '@codegenie/serverless-express';
import { app } from './app.js';
import { downloadMedia } from './services/downloadVideo.js';
import { s3Service } from './services/s3Service.js';
import { uploadToGemini } from './services/uploadToGemini.js';
import { sqsService } from './services/sqsService.js';

const httpHandler = serverlessExpress({ app });

/**
 * Unified AWS Lambda Handler for Media Extractor
 * Handles both:
 * 1. Background SQS events (Downloads video, saves to S3, uploads to Gemini, and dispatches to backend SQS)
 * 2. HTTP web traffic (Express REST APIs via Function URL / API Gateway)
 */
export const handler = async (event, context) => {
  // 1. If triggered by an AWS SQS queue event:
  if (event?.Records && event.Records.length > 0 && event.Records[0]?.eventSource === 'aws:sqs') {
    console.log(`[MediaExtractor Worker] Processing ${event.Records.length} SQS record(s)...`);

    for (const record of event.Records) {
      let payload;
      try {
        payload = JSON.parse(record.body);
      } catch (parseErr) {
        console.error('[Worker] Failed to parse SQS body:', record.body);
        continue;
      }

      const {
        jobId,
        url,
        userId,
        type = 'auto',
        bucket,
        prefix,
        deleteFromS3 = false,
        uploadToGemini: wantGemini = true,
        label = 'workout_extraction'
      } = payload;

      console.log(`[MediaExtractor Worker] Processing Job ID: ${jobId} for URL: ${url}...`);

      const isTemporary = Boolean(deleteFromS3 || label === 'workout_extraction');
      const targetPrefix = prefix || (isTemporary ? 'temp/workouts' : 'media');
      let localDownloadedPath = null;

      try {
        // Step A: Download media (Twitter, YouTube, Instagram)
        const downloaded = await downloadMedia({ url, type });
        localDownloadedPath = downloaded.filePath;

        // Step B: Upload to Gemini File API (if video & requested)
        let geminiFile = null;
        if (wantGemini && downloaded.mediaType === 'video' && process.env.GEMINI_API_KEY) {
          try {
            console.log(`[Worker] Uploading video to Gemini File API...`);
            geminiFile = await uploadToGemini(downloaded.filePath, path.basename(downloaded.filePath));
            console.log(`[Worker] Gemini File ready: ${geminiFile.uri}`);
          } catch (geminiErr) {
            console.warn('[Worker] Gemini upload warning:', geminiErr.message);
          }
        }

        // Step C: Upload to AWS S3
        const cleanPrefix = targetPrefix.replace(/^\/+|\/+$/g, '');
        const filename = path.basename(downloaded.filePath);
        const s3Key = `${cleanPrefix}/${filename}`;

        console.log(`[Worker] Uploading to S3: ${s3Key}...`);
        const uploadResult = await s3Service.uploadFile({
          filePath: downloaded.filePath,
          key: s3Key,
          contentType: downloaded.contentType,
          bucket,
          tagging: isTemporary ? 'deleteAfterExtraction=true&purpose=workout_extraction' : undefined,
          metadata: {
            platform: downloaded.platform,
            media_type: downloaded.mediaType,
            original_url: url,
            delete_after_extraction: String(isTemporary),
            ...(label ? { label } : {}),
            ...(userId ? { user_id: String(userId) } : {})
          }
        });

        // Step D: Dispatch result payload directly to freakyfit-backend SQS queue!
        if (sqsService.isBackendQueueConfigured()) {
          console.log(`[Worker] Dispatching extraction result to backend SQS...`);
          await sqsService.dispatchToBackend({
            jobId,
            userId,
            url,
            geminiFileUri: geminiFile?.uri || null,
            geminiFileName: geminiFile?.name || null,
            s3Key: uploadResult.key,
            s3Url: uploadResult.s3Url,
            deleteFromS3: isTemporary,
            platform: downloaded.platform,
            title: downloaded.title
          });
        }

        console.log(`[Worker] Job ${jobId} finished successfully!`);

      } catch (err) {
        console.error(`[Worker] Error processing job ${jobId}:`, err);
      } finally {
        // Step E: Clean up local disk file in /tmp
        if (localDownloadedPath && fs.existsSync(localDownloadedPath)) {
          try {
            fs.unlinkSync(localDownloadedPath);
          } catch (cleanErr) {
            console.warn('[Worker] Cleanup error:', cleanErr.message);
          }
        }
      }
    }

    return { statusCode: 200, body: 'Batch processing finished' };
  }

  // 2. Otherwise, route HTTP request through Express
  return httpHandler(event, context);
};
