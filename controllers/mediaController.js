import path from 'path';
import fs from 'fs';
import { downloadMedia, detectPlatform } from '../services/downloadVideo.js';
import { s3Service } from '../services/s3Service.js';
import { uploadToGemini } from '../services/uploadToGemini.js';
import { sqsService } from '../services/sqsService.js';

/**
 * Controller to download media, save to S3, optionally upload to Gemini, and notify backend SQS
 * POST /api/media/download
 */
export const downloadAndSaveMedia = async (req, res) => {
  const {
    url,
    type,
    bucket,
    prefix,
    deleteFromS3 = false,
    uploadToGemini: wantGemini = false,
    label,
    jobId,
    userId
  } = req.body;

  if (!url || typeof url !== 'string' || !url.trim()) {
    return res.status(400).json({
      success: false,
      error: 'Missing or invalid "url" in request body. Must be a valid Twitter, YouTube, or Instagram URL.'
    });
  }

  const isTemporary = Boolean(deleteFromS3 || label === 'workout_extraction');
  const targetPrefix = prefix || (isTemporary ? 'temp/workouts' : 'media');
  const shouldUploadToGemini = Boolean(wantGemini || label === 'workout_extraction');

  let localDownloadedPath = null;

  try {
    // 1. Download media (video or image) using unified downloadVideo service
    const downloaded = await downloadMedia({
      url: url.trim(),
      type: type || 'auto'
    });

    localDownloadedPath = downloaded.filePath;

    // 2. (Optional) Upload to Gemini File API if requested or for workout extraction
    let geminiFile = null;
    if (shouldUploadToGemini && downloaded.mediaType === 'video' && process.env.GEMINI_API_KEY) {
      try {
        console.log(`[MediaController] Uploading video to Gemini File API for workout extraction...`);
        geminiFile = await uploadToGemini(downloaded.filePath, path.basename(downloaded.filePath));
      } catch (geminiErr) {
        console.warn('[MediaController] Gemini upload error (proceeding with S3 upload):', geminiErr.message);
      }
    }

    // 3. Format S3 Key
    const cleanPrefix = targetPrefix.replace(/^\/+|\/+$/g, '');
    const filename = path.basename(downloaded.filePath);
    const s3Key = `${cleanPrefix}/${filename}`;

    console.log(`[MediaController] Uploading downloaded ${downloaded.platform} ${downloaded.mediaType} to S3: ${s3Key}`);

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
        original_url: url.trim(),
        delete_after_extraction: String(isTemporary),
        ...(label ? { label } : {}),
        ...(userId ? { user_id: String(userId) } : {})
      }
    });

    // 5. Notify freakyfit-backend SQS queue if configured and jobId is provided
    if (sqsService.isBackendQueueConfigured() && jobId) {
      await sqsService.dispatchToBackend({
        jobId,
        userId,
        url: url.trim(),
        geminiFileUri: geminiFile?.uri || null,
        geminiFileName: geminiFile?.name || null,
        s3Key: uploadResult.key,
        s3Url: uploadResult.s3Url,
        deleteFromS3: isTemporary,
        platform: downloaded.platform,
        title: downloaded.title
      });
    }

    return res.status(201).json({
      success: true,
      platform: downloaded.platform,
      mediaType: downloaded.mediaType,
      key: uploadResult.key,
      bucket: uploadResult.bucket,
      s3Url: uploadResult.s3Url,
      contentType: uploadResult.contentType,
      fileSize: uploadResult.size,
      title: downloaded.title,
      originalUrl: url.trim(),
      deleteFromS3: isTemporary,
      geminiFileUri: geminiFile?.uri || null,
      geminiFileName: geminiFile?.name || null
    });

  } catch (error) {
    console.error('[MediaController] Download & Save error:', error);
    return res.status(500).json({
      success: false,
      error: error.message || 'Failed to download media and upload to S3',
      platform: detectPlatform(url)
    });
  } finally {
    // 6. Ensure temporary local file is cleaned up from /tmp
    if (localDownloadedPath && fs.existsSync(localDownloadedPath)) {
      try {
        fs.unlinkSync(localDownloadedPath);
      } catch (cleanErr) {
        console.warn('[MediaController] Error cleaning up file:', cleanErr.message);
      }
    }
  }
};

/**
 * Controller to stream / load video or media directly from AWS S3
 * GET /api/media/:key(*) or GET /api/media?key=...
 */
export const getMedia = async (req, res) => {
  const key = req.params.key || req.params[0] || req.query.key;
  const bucket = req.query.bucket;
  const isDownload = req.query.download === 'true' || req.query.download === '1';
  const wantRedirect = req.query.redirect === 'true' || req.query.redirect === '1';

  if (!key) {
    return res.status(400).json({
      success: false,
      error: 'Missing S3 object key. Provide key in URL path (/api/media/<key>) or as query parameter (?key=<key>).'
    });
  }

  try {
    if (wantRedirect) {
      const presignedUrl = await s3Service.getPresignedUrl({ key, bucket });
      return res.redirect(presignedUrl);
    }

    const rangeHeader = req.headers.range;
    const {
      stream,
      contentType,
      contentLength,
      contentRange,
      acceptRanges,
      etag,
      lastModified
    } = await s3Service.getObject({
      key,
      bucket,
      range: rangeHeader
    });

    const filename = path.basename(key);

    res.setHeader('Content-Type', contentType || 'application/octet-stream');
    res.setHeader('Accept-Ranges', acceptRanges || 'bytes');
    if (etag) res.setHeader('ETag', etag);
    if (lastModified) res.setHeader('Last-Modified', new Date(lastModified).toUTCString());
    res.setHeader('Cache-Control', 'public, max-age=86400');

    if (isDownload) {
      res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    } else {
      res.setHeader('Content-Disposition', `inline; filename="${filename}"`);
    }

    // Support HTTP 206 Partial Content for video seeking/scrubbing
    if (rangeHeader && contentRange) {
      res.status(206);
      res.setHeader('Content-Range', contentRange);
      if (contentLength) res.setHeader('Content-Length', contentLength);
    } else {
      res.status(200);
      if (contentLength) res.setHeader('Content-Length', contentLength);
    }

    stream.pipe(res);

    stream.on('error', (streamErr) => {
      console.error('[MediaController] S3 stream error:', streamErr);
      if (!res.headersSent) {
        res.status(500).end();
      }
    });

  } catch (error) {
    console.error(`[MediaController] Error retrieving S3 object "${key}":`, error.name, error.message);

    if (error.name === 'NoSuchKey' || error.name === 'NotFound' || error.$metadata?.httpStatusCode === 404) {
      return res.status(404).json({
        success: false,
        error: `Media not found in S3 for key: ${key}`
      });
    }

    return res.status(500).json({
      success: false,
      error: error.message || 'Failed to retrieve media from S3'
    });
  }
};

/**
 * Controller to get a pre-signed URL for direct playback/loading from S3
 * GET /api/media/url/:key(*) or GET /api/media/url?key=...
 */
export const getMediaUrl = async (req, res) => {
  const key = req.params.key || req.params[0] || req.query.key;
  const bucket = req.query.bucket;
  const expiresIn = parseInt(req.query.expiresIn || '3600', 10);

  if (!key) {
    return res.status(400).json({
      success: false,
      error: 'Missing S3 object key. Provide key in URL path (/api/media/url/<key>) or as query parameter (?key=<key>).'
    });
  }

  try {
    const presignedUrl = await s3Service.getPresignedUrl({
      key,
      bucket,
      expiresIn: isNaN(expiresIn) ? 3600 : expiresIn
    });

    const bucketName = await s3Service.getBucketName(bucket);

    return res.status(200).json({
      success: true,
      key,
      bucket: bucketName,
      presignedUrl,
      expiresIn: isNaN(expiresIn) ? 3600 : expiresIn
    });
  } catch (error) {
    console.error(`[MediaController] Error generating presigned URL for "${key}":`, error);
    return res.status(500).json({
      success: false,
      error: error.message || 'Failed to generate pre-signed URL'
    });
  }
};

/**
 * Controller to delete media from AWS S3
 * DELETE /api/media/:key(*) or DELETE /api/media?key=...
 */
export const deleteMedia = async (req, res) => {
  const key = req.params.key || req.params[0] || req.query.key;
  const bucket = req.query.bucket;

  if (!key) {
    return res.status(400).json({
      success: false,
      error: 'Missing S3 object key. Provide key in URL path (/api/media/<key>) or as query parameter (?key=<key>).'
    });
  }

  try {
    await s3Service.deleteObject({ key, bucket });
    return res.status(200).json({
      success: true,
      message: `Object "${key}" successfully deleted from S3.`,
      key
    });
  } catch (error) {
    console.error(`[MediaController] Error deleting S3 object "${key}":`, error);
    return res.status(500).json({
      success: false,
      error: error.message || 'Failed to delete object from S3'
    });
  }
};
