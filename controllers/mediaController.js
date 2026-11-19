import path from 'path';
import { s3Service } from '../services/s3Service.js';
import { processMediaPipeline } from '../services/mediaPipeline.js';
import { detectPlatform } from '../services/downloadVideo.js';

/**
 * Controller to download media, save to S3, optionally upload to Gemini, and notify backend SQS
 * POST /api/media/download
 */
export const downloadAndSaveMedia = async (req, res) => {
  const { url } = req.body || {};

  if (!url || typeof url !== 'string' || !url.trim()) {
    return res.status(400).json({
      success: false,
      error: 'Missing or invalid "url" in request body. Must be a valid Twitter, YouTube, or Instagram URL.'
    });
  }

  try {
    const result = await processMediaPipeline(req.body);
    return res.status(201).json(result);
  } catch (error) {
    return res.status(500).json({
      success: false,
      error: error.message || 'Failed to download media and upload to S3',
      platform: detectPlatform(url)
    });
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
