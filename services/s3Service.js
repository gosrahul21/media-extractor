import {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
  HeadObjectCommand,
  DeleteObjectCommand
} from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import dotenv from 'dotenv';
import fs from 'fs';
import mime from 'mime-types';
import { configService } from './configService.js';

dotenv.config();

const region = process.env.AWS_REGION || process.env.AWS_DEFAULT_REGION || 'ap-southeast-2';

const s3Client = new S3Client({
  region,
  credentials: process.env.AWS_ACCESS_KEY_ID && process.env.AWS_SECRET_ACCESS_KEY ? {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
  } : undefined
});

export const s3Service = {
  getRegion: () => region,

  /**
   * Determine the target S3 bucket name
   */
  async getBucketName(overrideBucket) {
    if (overrideBucket) return overrideBucket;
    if (process.env.AWS_S3_BUCKET) return process.env.AWS_S3_BUCKET;
    if (process.env.S3_BUCKET_NAME) return process.env.S3_BUCKET_NAME;
    if (process.env.S3_BUCKET) return process.env.S3_BUCKET;

    const dynamicBucket = await configService.get('s3_bucket');
    if (dynamicBucket) return dynamicBucket;

    return 'freakyfit-media';
  },

  /**
   * Upload a local file, buffer, or stream to AWS S3
   */
  async uploadFile({ filePath, fileBuffer, fileStream, key, contentType, metadata = {}, tagging, bucket }) {
    const bucketName = await this.getBucketName(bucket);
    let body = fileBuffer || fileStream;
    let size = null;

    if (filePath) {
      body = fs.createReadStream(filePath);
      const stat = fs.statSync(filePath);
      size = stat.size;
      if (!contentType) {
        contentType = mime.lookup(filePath) || 'application/octet-stream';
      }
    }

    const command = new PutObjectCommand({
      Bucket: bucketName,
      Key: key,
      Body: body,
      ContentType: contentType || 'application/octet-stream',
      Metadata: metadata,
      ...(tagging ? { Tagging: tagging } : {})
    });

    const response = await s3Client.send(command);
    const s3Url = `https://${bucketName}.s3.${region}.amazonaws.com/${key}`;

    return {
      success: true,
      bucket: bucketName,
      key,
      s3Url,
      contentType: contentType || 'application/octet-stream',
      size,
      etag: response.ETag
    };
  },

  /**
   * Retrieve an object stream from S3 with optional HTTP Range header support
   */
  async getObject({ key, bucket, range }) {
    const bucketName = await this.getBucketName(bucket);
    const command = new GetObjectCommand({
      Bucket: bucketName,
      Key: key,
      ...(range ? { Range: range } : {})
    });

    const response = await s3Client.send(command);
    return {
      stream: response.Body,
      contentType: response.ContentType,
      contentLength: response.ContentLength,
      contentRange: response.ContentRange,
      acceptRanges: response.AcceptRanges,
      etag: response.ETag,
      lastModified: response.LastModified
    };
  },

  /**
   * Generate a pre-signed URL for direct playback or download
   */
  async getPresignedUrl({ key, bucket, expiresIn = 3600 }) {
    const bucketName = await this.getBucketName(bucket);
    const command = new GetObjectCommand({
      Bucket: bucketName,
      Key: key
    });
    return await getSignedUrl(s3Client, command, { expiresIn });
  },

  /**
   * Get object metadata (HeadObject)
   */
  async headObject({ key, bucket }) {
    const bucketName = await this.getBucketName(bucket);
    const command = new HeadObjectCommand({
      Bucket: bucketName,
      Key: key
    });
    return await s3Client.send(command);
  },

  /**
   * Delete object from S3
   */
  async deleteObject({ key, bucket }) {
    const bucketName = await this.getBucketName(bucket);
    const command = new DeleteObjectCommand({
      Bucket: bucketName,
      Key: key
    });
    return await s3Client.send(command);
  }
};
