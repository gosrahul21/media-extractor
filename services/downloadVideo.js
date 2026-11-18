import fs from 'fs';
import os from 'os';
import path from 'path';
import { pipeline } from 'stream/promises';
import { Readable } from 'stream';
import { create } from 'youtube-dl-exec';
import { fileURLToPath } from 'url';
import mime from 'mime-types';
import { configService } from './configService.js';

import dotenv from 'dotenv';
dotenv.config();

const ytDlp = create('yt-dlp');
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export const TMP_DIR = process.env.AWS_LAMBDA_FUNCTION_NAME || process.env.LAMBDA_TASK_ROOT
  ? os.tmpdir()
  : path.join(__dirname, '../tmp');

if (!fs.existsSync(TMP_DIR)) {
  fs.mkdirSync(TMP_DIR, { recursive: true });
}

/**
 * Encode a Netscape-format cookies string into a single-line base64 string
 * suitable for the YOUTUBE_COOKIES environment variable.
 */
export const encodeCookiesToBase64 = (cookiesText) => {
  const normalized = cookiesText.replace(/\r\n/g, '\n').trim();
  if (!normalized) {
    throw new Error('Cookies content is empty');
  }
  return Buffer.from(normalized, 'utf-8').toString('base64');
};

/**
 * Read a Netscape cookies file (default: ./cookies.txt) and encode it
 * into a single-line base64 string for YOUTUBE_COOKIES.
 */
export const encodeCookiesFileToBase64 = (
  cookiesFilePath = path.join(__dirname, '../cookies.txt')
) => {
  const resolvedPath = path.resolve(cookiesFilePath);
  const content = fs.readFileSync(resolvedPath, 'utf-8');
  return encodeCookiesToBase64(content);
};

/**
 * Identify platform from URL
 */
export const detectPlatform = (url) => {
  if (/twitter\.com|x\.com/i.test(url)) return 'twitter';
  if (/youtube\.com|youtu\.be/i.test(url)) return 'youtube';
  if (/instagram\.com/i.test(url)) return 'instagram';
  return 'other';
};

/**
 * Resolve dynamic cookies from Supabase or environment
 */
export const resolveCookies = async (url) => {
  const isYouTube = /youtube\.com|youtu\.be/i.test(url);
  const isInstagram = /instagram\.com/i.test(url);

  let dynamicCookies = null;
  let platformLabel = 'General';

  if (isYouTube) {
    dynamicCookies = await configService.get('youtube_cookies');
    platformLabel = 'YouTube';
  } else if (isInstagram) {
    dynamicCookies = (await configService.get('instagram_cookies')) || (await configService.get('youtube_cookies'));
    platformLabel = 'Instagram';
  }

  const rawCookies = (dynamicCookies || (isYouTube ? process.env.YOUTUBE_COOKIES : '') || '').trim();

  if (rawCookies) {
    const envCookiesPath = path.join(TMP_DIR, 'env_cookies.txt');
    try {
      const content = (rawCookies.startsWith('# Netscape') || rawCookies.includes('\t'))
        ? rawCookies
        : Buffer.from(rawCookies, 'base64').toString('utf-8');
      fs.writeFileSync(envCookiesPath, content, 'utf-8');
      return { cookiesPath: envCookiesPath, platformLabel };
    } catch (e) {
      console.error('Failed to parse cookies:', e.message);
    }
  }

  return { cookiesPath: null, platformLabel };
};

/**
 * Direct file download from URL (images, direct video streams)
 */
async function downloadDirectFile(fileUrl, destinationPath) {
  const res = await fetch(fileUrl, {
    headers: {
      'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
  });

  if (!res.ok) {
    throw new Error(`Failed to download from ${fileUrl}: ${res.status} ${res.statusText}`);
  }

  const fileStream = fs.createWriteStream(destinationPath);
  await pipeline(Readable.fromWeb(res.body), fileStream);

  let contentType = res.headers.get('content-type') || mime.lookup(destinationPath) || 'application/octet-stream';
  if (contentType.includes(';')) {
    contentType = contentType.split(';')[0].trim();
  }

  const stat = fs.statSync(destinationPath);

  return {
    filePath: destinationPath,
    contentType,
    size: stat.size
  };
}

/**
 * Fetch Twitter metadata & direct media URLs via syndication / vx API
 */
async function fetchTwitterMetadata(tweetUrl) {
  const match = tweetUrl.match(/(?:twitter\.com|x\.com)\/(?:#!\/)?(\w+)\/status(?:es)?\/(\d+)/i);
  if (!match) return null;

  const user = match[1] || 'Twitter';
  const tweetId = match[2];

  try {
    const response = await fetch(`https://api.vxtwitter.com/${user}/status/${tweetId}`, {
      headers: {
        'User-Agent': 'TelegramBot'
      }
    });

    if (response.ok) {
      return await response.json();
    }
  } catch (err) {
    console.warn(`[downloadVideo] VxTwitter lookup failed:`, err.message);
  }

  return null;
}

/**
 * Download video specifically for AI Workout Extraction (preserves 2-minute duration limit)
 */
export const downloadVideo = async (url, filePath) => {
  console.log(`[1/4] Downloading video from ${url}...`);

  const isYouTube = /youtube\.com|youtu\.be/i.test(url);
  const { cookiesPath, platformLabel } = await resolveCookies(url);

  const baseOptions = {
    jsRuntimes: 'node',
    remoteComponents: 'ejs:github'
  };

  if (cookiesPath) {
    console.log(`Using cookies for ${platformLabel} authentication...`);
  }

  const attempts = [
    {
      label: cookiesPath ? `default clients + ${platformLabel} cookies` : 'default clients',
      options: {
        ...baseOptions,
        ...(cookiesPath ? { cookies: cookiesPath } : {})
      }
    },
    ...(isYouTube
      ? [
          {
            label: cookiesPath ? 'tv,web_embedded + cookies' : 'tv,web_embedded',
            options: {
              ...baseOptions,
              extractorArgs: 'youtube:player_client=tv,web_embedded',
              ...(cookiesPath ? { cookies: cookiesPath } : {})
            }
          }
        ]
      : []),
    ...(cookiesPath
      ? [
          {
            label: 'default clients (no cookies)',
            options: { ...baseOptions }
          }
        ]
      : [])
  ];

  // 1. Fetch metadata to extract thumbnail and validate video duration
  let thumbnailUrl = null;
  let preferredAttempts = attempts;
  for (let i = 0; i < attempts.length; i++) {
    const attempt = attempts[i];
    try {
      const metadata = await ytDlp(url, {
        dumpJson: true,
        ...attempt.options
      });

      // Reject videos longer than 2 minutes (120 seconds) before downloading
      const durationSeconds = metadata?.duration;
      if (typeof durationSeconds === 'number' && durationSeconds > 120) {
        const minutes = Math.floor(durationSeconds / 60);
        const seconds = Math.round(durationSeconds % 60);
        const formatted = `${minutes}m ${seconds}s`;
        const err = new Error(`Video duration (${formatted}) exceeds the 2-minute limit.`);
        err.code = 'VIDEO_TOO_LONG';
        err.stage = 'download';
        err.duration = durationSeconds;
        throw err;
      }

      thumbnailUrl = metadata.thumbnail || null;
      console.log(`[1/4] Extracted thumbnail (${attempt.label}): ${thumbnailUrl}`);
      preferredAttempts = [attempt, ...attempts.filter((_, idx) => idx !== i)];
      break;
    } catch (err) {
      if (err.code === 'VIDEO_TOO_LONG') {
        throw err;
      }
      console.error(`Failed to extract metadata thumbnail (${attempt.label}):`, err.message);
    }
  }

  // 2. Download the video with fallback across client/cookie configs
  let lastError = null;
  for (const attempt of preferredAttempts) {
    try {
      await ytDlp(url, {
        output: filePath,
        format: 'best[height<=720]/bestvideo[height<=720]+bestaudio/best',
        mergeOutputFormat: 'mp4',
        ...attempt.options
      });
      return thumbnailUrl;
    } catch (err) {
      lastError = err;
      console.error(`[1/4] Download failed (${attempt.label}):`, err.message);
    }
  }

  throw lastError;
};

/**
 * Universal Media Downloader: Downloads videos OR images from Twitter, YouTube, Instagram or direct URLs
 *
 * @param {Object} params
 * @param {string} params.url - URL to download
 * @param {string} [params.type='auto'] - 'video' | 'image' | 'auto'
 * @param {string} [params.baseOutput] - Base filename prefix without extension
 * @returns {Promise<{ filePath: string, contentType: string, mediaType: string, platform: string, title?: string, size: number }>}
 */
export const downloadMedia = async ({ url, type = 'auto', baseOutput }) => {
  const platform = detectPlatform(url);
  const uniqueId = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  const outputPrefix = baseOutput || path.join(TMP_DIR, `media-${uniqueId}`);
  let downloadedFilePath = null;
  let contentType = null;
  let detectedType = type;
  let mediaTitle = null;

  console.log(`[downloadVideo] Downloading ${platform} media from ${url} (type: ${type})...`);

  const { cookiesPath } = await resolveCookies(url);
  const baseOptions = {
    jsRuntimes: 'node',
    remoteComponents: 'ejs:github',
    ...(cookiesPath ? { cookies: cookiesPath } : {})
  };

  // 1. TWITTER / X
  if (platform === 'twitter') {
    const twitterData = await fetchTwitterMetadata(url);
    mediaTitle = twitterData?.text || 'Twitter Media';

    const mediaExtended = twitterData?.media_extended || [];
    const hasVideos = mediaExtended.filter(m => m.type === 'video' || m.type === 'gif');
    const hasImages = mediaExtended.filter(m => m.type === 'image');

    if (type === 'image' && hasImages.length > 0) {
      const imgUrl = hasImages[0].url;
      const ext = path.extname(new URL(imgUrl).pathname) || '.jpg';
      const localPath = `${outputPrefix}${ext}`;
      const res = await downloadDirectFile(imgUrl, localPath);
      downloadedFilePath = res.filePath;
      contentType = res.contentType;
      detectedType = 'image';
    } else if ((type === 'video' || type === 'auto') && hasVideos.length > 0) {
      const vidUrl = hasVideos[0].url;
      const localPath = `${outputPrefix}.mp4`;
      const res = await downloadDirectFile(vidUrl, localPath);
      downloadedFilePath = res.filePath;
      contentType = 'video/mp4';
      detectedType = 'video';
    } else if (hasImages.length > 0 && type === 'auto') {
      const imgUrl = hasImages[0].url;
      const ext = path.extname(new URL(imgUrl).pathname) || '.jpg';
      const localPath = `${outputPrefix}${ext}`;
      const res = await downloadDirectFile(imgUrl, localPath);
      downloadedFilePath = res.filePath;
      contentType = res.contentType;
      detectedType = 'image';
    } else {
      // yt-dlp fallback for Twitter
      try {
        await ytDlp(url, {
          output: `${outputPrefix}.%(ext)s`,
          format: 'best[height<=1080]/bestvideo+bestaudio/best',
          mergeOutputFormat: 'mp4',
          ...baseOptions
        });

        const files = fs.readdirSync(path.dirname(outputPrefix)).filter(f => f.startsWith(path.basename(outputPrefix)));
        if (files.length > 0) {
          downloadedFilePath = path.join(path.dirname(outputPrefix), files[0]);
          contentType = mime.lookup(downloadedFilePath) || 'video/mp4';
          detectedType = contentType.startsWith('image/') ? 'image' : 'video';
        }
      } catch (ytErr) {
        if (/No video could be found/i.test(ytErr.message) || type === 'image') {
          await ytDlp(url, {
            output: `${outputPrefix}.%(ext)s`,
            writeThumbnail: true,
            skipDownload: true,
            ...baseOptions
          });
          const files = fs.readdirSync(path.dirname(outputPrefix)).filter(f => f.startsWith(path.basename(outputPrefix)));
          if (files.length > 0) {
            downloadedFilePath = path.join(path.dirname(outputPrefix), files[0]);
            contentType = mime.lookup(downloadedFilePath) || 'image/jpeg';
            detectedType = 'image';
          } else {
            throw ytErr;
          }
        } else {
          throw ytErr;
        }
      }
    }
  }

  // 2. YOUTUBE
  else if (platform === 'youtube') {
    if (type === 'image') {
      await ytDlp(url, {
        output: `${outputPrefix}.%(ext)s`,
        writeThumbnail: true,
        skipDownload: true,
        ...baseOptions
      });

      const files = fs.readdirSync(path.dirname(outputPrefix)).filter(f => f.startsWith(path.basename(outputPrefix)));
      if (files.length > 0) {
        downloadedFilePath = path.join(path.dirname(outputPrefix), files[0]);
        contentType = mime.lookup(downloadedFilePath) || 'image/jpeg';
        detectedType = 'image';
      }
    } else {
      const metadata = await ytDlp(url, {
        dumpJson: true,
        ...baseOptions
      }).catch(() => null);

      mediaTitle = metadata?.title || 'YouTube Media';

      await ytDlp(url, {
        output: `${outputPrefix}.%(ext)s`,
        format: 'best[height<=1080]/bestvideo[height<=1080]+bestaudio/best',
        mergeOutputFormat: 'mp4',
        ...baseOptions
      });

      const files = fs.readdirSync(path.dirname(outputPrefix)).filter(f => f.startsWith(path.basename(outputPrefix)));
      if (files.length > 0) {
        downloadedFilePath = path.join(path.dirname(outputPrefix), files[0]);
        contentType = mime.lookup(downloadedFilePath) || 'video/mp4';
        detectedType = 'video';
      }
    }
  }

  // 3. INSTAGRAM
  else if (platform === 'instagram') {
    if (type === 'image') {
      await ytDlp(url, {
        output: `${outputPrefix}.%(ext)s`,
        writeThumbnail: true,
        skipDownload: true,
        ...baseOptions
      });

      const files = fs.readdirSync(path.dirname(outputPrefix)).filter(f => f.startsWith(path.basename(outputPrefix)));
      if (files.length > 0) {
        downloadedFilePath = path.join(path.dirname(outputPrefix), files[0]);
        contentType = mime.lookup(downloadedFilePath) || 'image/jpeg';
        detectedType = 'image';
      }
    } else {
      try {
        await ytDlp(url, {
          output: `${outputPrefix}.%(ext)s`,
          format: 'best[height<=1080]/bestvideo+bestaudio/best',
          mergeOutputFormat: 'mp4',
          ...baseOptions
        });

        const files = fs.readdirSync(path.dirname(outputPrefix)).filter(f => f.startsWith(path.basename(outputPrefix)));
        if (files.length > 0) {
          downloadedFilePath = path.join(path.dirname(outputPrefix), files[0]);
          contentType = mime.lookup(downloadedFilePath) || 'video/mp4';
          detectedType = contentType.startsWith('image/') ? 'image' : 'video';
        }
      } catch (igErr) {
        console.log(`[downloadVideo] Instagram video failed, fallback to photo: ${igErr.message}`);
        await ytDlp(url, {
          output: `${outputPrefix}.%(ext)s`,
          writeThumbnail: true,
          skipDownload: true,
          ...baseOptions
        });

        const files = fs.readdirSync(path.dirname(outputPrefix)).filter(f => f.startsWith(path.basename(outputPrefix)));
        if (files.length > 0) {
          downloadedFilePath = path.join(path.dirname(outputPrefix), files[0]);
          contentType = mime.lookup(downloadedFilePath) || 'image/jpeg';
          detectedType = 'image';
        } else {
          throw igErr;
        }
      }
    }
  }

  // 4. DIRECT URL / OTHER
  else {
    const parsedUrl = new URL(url);
    const ext = path.extname(parsedUrl.pathname) || (type === 'image' ? '.jpg' : '.mp4');
    const localPath = `${outputPrefix}${ext}`;
    const res = await downloadDirectFile(url, localPath);
    downloadedFilePath = res.filePath;
    contentType = res.contentType;
    detectedType = contentType.startsWith('image/') ? 'image' : 'video';
  }

  if (!downloadedFilePath || !fs.existsSync(downloadedFilePath)) {
    throw new Error(`Failed to locate downloaded file for ${url}`);
  }

  const stat = fs.statSync(downloadedFilePath);

  return {
    filePath: downloadedFilePath,
    contentType,
    mediaType: detectedType,
    platform,
    title: mediaTitle,
    size: stat.size
  };
};
