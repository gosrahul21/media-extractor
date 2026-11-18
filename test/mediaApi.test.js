import assert from 'assert';
import { app } from '../app.js';
import { s3Service } from '../services/s3Service.js';
import { detectPlatform } from '../services/downloadVideo.js';
import { Readable } from 'stream';

async function runTests() {
  console.log('--- Running Media Extractor Tests ---');

  // 1. Platform Detection
  console.log('1. Testing Platform Detection...');
  assert.strictEqual(detectPlatform('https://x.com/jack/status/20'), 'twitter');
  assert.strictEqual(detectPlatform('https://twitter.com/jack/status/20'), 'twitter');
  assert.strictEqual(detectPlatform('https://www.youtube.com/watch?v=dQw4w9WgXcQ'), 'youtube');
  assert.strictEqual(detectPlatform('https://youtu.be/dQw4w9WgXcQ'), 'youtube');
  assert.strictEqual(detectPlatform('https://www.instagram.com/reel/C1234567/'), 'instagram');
  assert.strictEqual(detectPlatform('https://example.com/image.png'), 'other');
  console.log('   Platform detection passed!');

  const server = app.listen(0);
  const port = server.address().port;
  const baseUrl = `http://localhost:${port}`;

  try {
    // 2. Health & Validation
    console.log('2. Testing Health & Validation...');
    const resHealth = await fetch(`${baseUrl}/health`).then(r => r.json());
    assert.strictEqual(resHealth.success, true);

    const resEmptyPost = await fetch(`${baseUrl}/api/media/download`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({})
    });
    assert.strictEqual(resEmptyPost.status, 400);

    const resNoKeyMedia = await fetch(`${baseUrl}/api/media`);
    assert.strictEqual(resNoKeyMedia.status, 400);

    const resNoKeyUrl = await fetch(`${baseUrl}/api/media/url`);
    assert.strictEqual(resNoKeyUrl.status, 400);
    console.log('   Health & validation passed!');

    // 3. Presigned URL Generation
    console.log('3. Testing Presigned URL generation endpoint...');
    const resUrl = await fetch(`${baseUrl}/api/media/url/media/test-video.mp4`);
    assert.strictEqual(resUrl.status, 200);
    const urlData = await resUrl.json();
    assert.strictEqual(urlData.success, true);
    assert.strictEqual(urlData.key, 'media/test-video.mp4');
    assert.ok(urlData.presignedUrl.includes('test-video.mp4'));
    console.log('   Presigned URL generation passed!');

    // 4. Media Streaming & Range Handling
    console.log('4. Testing Media Streaming & Range handling...');
    const originalGetObject = s3Service.getObject;
    const dummyData = Buffer.from('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789');

    s3Service.getObject = async ({ key, range }) => {
      let streamData = dummyData;
      let contentRange = undefined;

      if (range) {
        const match = range.match(/bytes=(\d+)-(\d+)?/);
        if (match) {
          const start = parseInt(match[1], 10);
          const end = match[2] ? parseInt(match[2], 10) : dummyData.length - 1;
          streamData = dummyData.subarray(start, end + 1);
          contentRange = `bytes ${start}-${end}/${dummyData.length}`;
        }
      }

      return {
        stream: Readable.from(streamData),
        contentType: 'video/mp4',
        contentLength: streamData.length,
        contentRange,
        acceptRanges: 'bytes',
        etag: '"test-etag"',
        lastModified: new Date()
      };
    };

    const resStream = await fetch(`${baseUrl}/api/media/media/demo.mp4`);
    assert.strictEqual(resStream.status, 200);
    assert.strictEqual(resStream.headers.get('content-type'), 'video/mp4');

    const resRange = await fetch(`${baseUrl}/api/media/media/demo.mp4`, {
      headers: { Range: 'bytes=0-9' }
    });
    assert.strictEqual(resRange.status, 206);
    assert.strictEqual(await resRange.text(), 'ABCDEFGHIJ');

    s3Service.getObject = originalGetObject;
    console.log('   Streaming & Range requests passed!');

    // 5. Download & S3 Upload Flow
    console.log('5. Testing Download & Save to S3 flow...');
    const originalUpload = s3Service.uploadFile;
    s3Service.uploadFile = async ({ key, contentType, bucket }) => ({
      success: true,
      bucket: bucket || 'freakyfit-media',
      key,
      s3Url: `https://freakyfit-media.s3.amazonaws.com/${key}`,
      contentType,
      size: 15921
    });

    const resDownload = await fetch(`${baseUrl}/api/media/download`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        url: 'https://i.ytimg.com/vi/jNQXAC9IVRw/hqdefault.jpg',
        type: 'image',
        prefix: 'unit-tests'
      })
    });

    assert.strictEqual(resDownload.status, 201);
    const downloadData = await resDownload.json();
    assert.strictEqual(downloadData.success, true);
    assert.strictEqual(downloadData.mediaType, 'image');

    s3Service.uploadFile = originalUpload;
    console.log('   Download & Save passed!');

    // 6. Delete Endpoint
    console.log('6. Testing S3 media deletion...');
    const originalDelete = s3Service.deleteObject;
    let deletedKey = null;
    s3Service.deleteObject = async ({ key }) => { deletedKey = key; return {}; };

    const resDelete = await fetch(`${baseUrl}/api/media/temp/workouts/sample.mp4`, {
      method: 'DELETE'
    });
    assert.strictEqual(resDelete.status, 200);
    assert.strictEqual(deletedKey, 'temp/workouts/sample.mp4');

    s3Service.deleteObject = originalDelete;
    console.log('   S3 deletion passed!');

    // 7. SQS Dispatch to Backend Queue
    console.log('7. Testing SQS notification to backend...');
    const { sqsService } = await import('../services/sqsService.js');
    const originalIsConfigured = sqsService.isBackendQueueConfigured;
    const originalDispatch = sqsService.dispatchToBackend;

    sqsService.isBackendQueueConfigured = () => true;
    let dispatchedPayload = null;
    sqsService.dispatchToBackend = async (payload) => {
      dispatchedPayload = payload;
      return { MessageId: 'msg-999' };
    };

    s3Service.uploadFile = async ({ key, contentType, bucket }) => ({
      success: true,
      bucket: bucket || 'freakyfit-media',
      key,
      s3Url: `https://freakyfit-media.s3.amazonaws.com/${key}`,
      contentType,
      size: 15921
    });

    const resSqsDispatch = await fetch(`${baseUrl}/api/media/download`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        url: 'https://i.ytimg.com/vi/jNQXAC9IVRw/hqdefault.jpg',
        type: 'image',
        jobId: 'job-12345',
        userId: 'user-abc',
        deleteFromS3: true
      })
    });

    assert.strictEqual(resSqsDispatch.status, 201);
    assert.strictEqual(dispatchedPayload.jobId, 'job-12345');
    assert.strictEqual(dispatchedPayload.userId, 'user-abc');
    assert.strictEqual(dispatchedPayload.deleteFromS3, true);

    sqsService.isBackendQueueConfigured = originalIsConfigured;
    sqsService.dispatchToBackend = originalDispatch;
    s3Service.uploadFile = originalUpload;
    console.log('   SQS notification passed!');

    console.log('\n ALL TESTS PASSED SUCCESSFULLY! ');
  } finally {
    server.close();
  }
}

runTests().catch(err => {
  console.error('Test error:', err);
  process.exit(1);
});
