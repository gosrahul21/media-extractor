import express from 'express';
import dotenv from 'dotenv';
import {
  downloadAndSaveMedia,
  getMedia,
  getMediaUrl,
  deleteMedia
} from './controllers/mediaController.js';

dotenv.config();

export const app = express();
app.use(express.json());

// Public health checks
app.get('/', (req, res) => {
  res.send({
    status: 'ok',
    service: 'freakyfit-media-extractor',
    description: 'Standalone Media Downloader, S3 Storage & Gemini Video Extractor'
  });
});

app.get('/health', (req, res) => {
  res.send({
    success: true,
    service: 'freakyfit-media-extractor',
    health: 'ok'
  });
});

// Media APIs: Download & Save to AWS S3, Load from AWS S3, and Delete from AWS S3
app.post('/api/media/download', downloadAndSaveMedia);
app.get('/api/media/url/:key(*)', getMediaUrl);
app.get('/api/media/url', getMediaUrl);
app.get('/api/media/:key(*)', getMedia);
app.get('/api/media', getMedia);
app.delete('/api/media/:key(*)', deleteMedia);
app.delete('/api/media', deleteMedia);
