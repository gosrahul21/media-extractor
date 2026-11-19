import serverlessExpress from '@codegenie/serverless-express';
import { app } from './app.js';
import { processMediaPipeline } from './services/mediaPipeline.js';

const httpHandler = serverlessExpress({ app });

/**
 * Unified AWS Lambda Handler for Media Extractor
 * Handles both:
 * 1. Background SQS events (Delegates to processMediaPipeline)
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

      console.log(`[Worker] Processing Job ID: ${payload.jobId} for URL: ${payload.url}...`);
      try {
        await processMediaPipeline(payload);
        console.log(`[Worker] Job ${payload.jobId} finished successfully!`);
      } catch (err) {
        console.error(`[Worker] Job ${payload.jobId} failed:`, err.message);
      }
    }

    return { statusCode: 200, body: 'Batch processing finished' };
  }

  // 2. Direct health check shortcut (handles Function URL, API Gateway, and raw Lambda console test events)
  const reqPath = event?.rawPath || event?.path || event?.requestContext?.http?.path;
  if (reqPath === '/health' || reqPath === '/health/' || reqPath === '/') {
    return {
      statusCode: 200,
      headers: {
        'content-type': 'application/json',
        'access-control-allow-origin': '*'
      },
      body: JSON.stringify({
        success: true,
        service: 'media-extractor',
        health: 'ok',
        timestamp: new Date().toISOString()
      })
    };
  }

  // 3. Otherwise, route HTTP request through Express with error boundary
  try {
    return await httpHandler(event, context);
  } catch (httpErr) {
    console.error('[Worker] HTTP handler error:', httpErr);
    return {
      statusCode: 500,
      headers: {
        'content-type': 'application/json',
        'access-control-allow-origin': '*'
      },
      body: JSON.stringify({
        success: false,
        error: 'Internal Server Error',
        message: httpErr.message
      })
    };
  }
};
