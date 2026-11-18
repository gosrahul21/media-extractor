import { SQSClient, SendMessageCommand } from '@aws-sdk/client-sqs';
import dotenv from 'dotenv';
dotenv.config();

const region = process.env.AWS_REGION || 'ap-southeast-2';
const backendQueueUrl = process.env.BACKEND_SQS_QUEUE_URL || process.env.LLM_SQS_QUEUE_URL;

const sqsClient = new SQSClient({ region });

export const sqsService = {
  isBackendQueueConfigured: () => Boolean(backendQueueUrl),

  /**
   * Dispatch extracted media & Gemini URI back to freakyfit-backend's SQS
   */
  async dispatchToBackend({
    jobId,
    userId,
    url,
    thumbnailUrl,
    geminiFileUri,
    geminiFileName,
    s3Key,
    s3Url,
    deleteFromS3,
    platform,
    title
  }) {
    if (!backendQueueUrl) {
      console.warn('[SQSService] BACKEND_SQS_QUEUE_URL is not configured. Skipping SQS notification.');
      return null;
    }

    const payload = {
      action: 'llm_workout_extraction',
      jobId,
      userId,
      url,
      thumbnailUrl,
      geminiFileUri,
      geminiFileName,
      s3Key,
      s3Url,
      deleteFromS3: Boolean(deleteFromS3),
      platform,
      title
    };

    const command = new SendMessageCommand({
      QueueUrl: backendQueueUrl,
      MessageBody: JSON.stringify(payload),
      MessageAttributes: {
        JobId: { DataType: 'String', StringValue: String(jobId || 'unknown') },
        Action: { DataType: 'String', StringValue: 'llm_workout_extraction' }
      }
    });

    const response = await sqsClient.send(command);
    console.log(`[SQSService] Dispatched extraction result for job ${jobId} to backend SQS: ${response.MessageId}`);
    return response;
  }
};
