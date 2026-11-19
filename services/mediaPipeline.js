import path from 'path';
import fs from 'fs';
import { downloadMedia, detectPlatform } from './downloadVideo.js';
import { s3Service } from './s3Service.js';
import { uploadToGemini } from './uploadToGemini.js';
import { sqsService } from './sqsService.js';
import { supabase } from '../config/supabaseClient.js';

/**
 * Unified Core Pipeline for Media Extraction
 * Downloads media from URL, uploads to Gemini & S3, notifies backend SQS, and manages cleanup.
 * Shared by both the background SQS worker and direct HTTP REST API.
 */
export const processMediaPipeline = async ({
  url,
  type = 'auto',
  bucket,
  prefix,
  deleteFromS3 = false,
  uploadToGemini: wantGemini,
  label = 'workout_extraction',
  jobId,
  userId
}) => {
  if (!url || typeof url !== 'string' || !url.trim()) {
    throw new Error('Missing or invalid "url". Must be a valid Twitter, YouTube, or Instagram URL.');
  }

  const cleanUrl = url.trim();
  const isTemporary = Boolean(deleteFromS3 || label === 'workout_extraction');
  const targetPrefix = prefix || (isTemporary ? 'temp/workouts' : 'media');
  const shouldUploadToGemini = typeof wantGemini === 'boolean'
    ? wantGemini
    : Boolean(label === 'workout_extraction');

  let localDownloadedPath = null;

  try {
    // 1. Download media (video or image)
    const downloaded = await downloadMedia({
      url: cleanUrl,
      type: type || 'auto'
    });
    localDownloadedPath = downloaded.filePath;

    // 2. (Optional) Upload to Gemini File API if requested
    let geminiFile = null;
    if (shouldUploadToGemini && downloaded.mediaType === 'video' && process.env.GEMINI_API_KEY) {
      try {
        console.log(`[MediaPipeline] Uploading video to Gemini File API...`);
        geminiFile = await uploadToGemini(downloaded.filePath, path.basename(downloaded.filePath));
        console.log(`[MediaPipeline] Gemini File ready: ${geminiFile?.uri}`);
      } catch (geminiErr) {
        console.warn('[MediaPipeline] Gemini upload warning (continuing with S3 upload):', geminiErr.message);
      }
    }

    // 3. Format S3 Key
    const cleanPrefix = targetPrefix.replace(/^\/+|\/+$/g, '');
    const filename = path.basename(downloaded.filePath);
    const s3Key = `${cleanPrefix}/${filename}`;

    console.log(`[MediaPipeline] Uploading ${downloaded.platform} ${downloaded.mediaType} to S3: ${s3Key}`);

    // 4. Upload to AWS S3 with tagging & metadata
    const uploadResult = await s3Service.uploadFile({
      filePath: downloaded.filePath,
      key: s3Key,
      contentType: downloaded.contentType,
      bucket,
      tagging: isTemporary ? 'deleteAfterExtraction=true&purpose=workout_extraction' : undefined,
      metadata: {
        platform: downloaded.platform,
        media_type: downloaded.mediaType,
        original_url: cleanUrl,
        delete_after_extraction: String(isTemporary),
        ...(label ? { label } : {}),
        ...(userId ? { user_id: String(userId) } : {})
      }
    });

    // 5. Notify backend SQS queue if configured and jobId is provided
    if (sqsService.isBackendQueueConfigured() && jobId) {
      console.log(`[MediaPipeline] Dispatching extraction result for Job ${jobId} to backend SQS...`);
      await sqsService.dispatchToBackend({
        jobId,
        userId,
        url: cleanUrl,
        geminiFileUri: geminiFile?.uri || null,
        geminiFileName: geminiFile?.name || null,
        s3Key: uploadResult.key,
        s3Url: uploadResult.s3Url,
        deleteFromS3: isTemporary,
        platform: downloaded.platform,
        title: downloaded.title
      });
    }

    return {
      success: true,
      platform: downloaded.platform,
      mediaType: downloaded.mediaType,
      key: uploadResult.key,
      bucket: uploadResult.bucket,
      s3Url: uploadResult.s3Url,
      contentType: uploadResult.contentType,
      fileSize: uploadResult.size,
      title: downloaded.title,
      originalUrl: cleanUrl,
      deleteFromS3: isTemporary,
      geminiFileUri: geminiFile?.uri || null,
      geminiFileName: geminiFile?.name || null
    };

  } catch (err) {
    console.error(`[MediaPipeline] Error processing media for Job ${jobId || 'direct'}:`, err.message);

    // Dispatch failure notification to backend SQS
    if (jobId && sqsService.isBackendQueueConfigured()) {
      try {
        await sqsService.dispatchFailureToBackend({
          jobId,
          userId,
          url: cleanUrl,
          error: err,
          platform: detectPlatform(cleanUrl)
        });
      } catch (sqsErr) {
        console.warn(`[MediaPipeline] SQS failure dispatch warning:`, sqsErr.message);
      }
    }

    // Directly update Supabase processing_jobs state if configured
    if (jobId && supabase) {
      try {
        await supabase
          .from('processing_jobs')
          .update({
            state: 'failed',
            failed_reason: err.message || 'Media extraction failed',
            error: { message: err.message },
            updated_at: new Date().toISOString()
          })
          .eq('id', jobId);
        console.log(`[MediaPipeline] Updated Supabase job ${jobId} to failed state.`);
      } catch (dbErr) {
        console.warn(`[MediaPipeline] Supabase update warning:`, dbErr.message);
      }
    }

    throw err;

  } finally {
    // 6. Clean up temporary local file in /tmp
    if (localDownloadedPath && fs.existsSync(localDownloadedPath)) {
      try {
        fs.unlinkSync(localDownloadedPath);
      } catch (cleanErr) {
        console.warn('[MediaPipeline] Cleanup error:', cleanErr.message);
      }
    }
  }
};
