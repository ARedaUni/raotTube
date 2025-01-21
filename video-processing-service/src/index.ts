import express, { Express, Request, Response } from 'express';
import ffmpeg from 'fluent-ffmpeg';
import { Storage } from '@google-cloud/storage';
import admin from 'firebase-admin';
import path from 'path';
import fs from 'fs';

// Configuration constants
const BUCKET_NAME = 'raot-tube-videos-processed';
const SUPPORTED_FORMATS = ['mp4', 'mov', 'avi', 'mkv'];
const PROCESSING_TIMEOUT = 900000; // 30 minutes
const VIDEO_QUALITIES = {
  '360p': { height: 360, crf: 23 },
  '720p': { height: 720, crf: 23 }
};

// Initialize Firebase Admin with explicit credential loading
// In production, Cloud Run will use the service account assigned to it
try {
  admin.initializeApp({
    credential: admin.credential.applicationDefault()
  });
} catch (error) {
  console.error('Failed to initialize Firebase Admin:', error);
  process.exit(1);
}

// Initialize Firestore
const db = admin.firestore();

// Initialize Google Cloud Storage with default credentials
const storage = new Storage();

// Create Express app
const app: Express = express();
app.use(express.json());

// Define TypeScript interfaces for better type safety
interface VideoMetadata {
  duration?: number;
  format?: string;
  width?: number;
  height?: number;
}

interface ProcessingJob {
  videoId: string;
  bucket: string;
  name: string;
}

// Enhanced video transcoding function with better error handling and logging
function transcodeVideo(
  inputPath: string, 
  outputPath: string, 
  quality: { height: number; crf: number }
): Promise<void> {
  return new Promise((resolve, reject) => {
    console.log(`Starting transcode for quality ${quality.height}p...`);
    
    const command = ffmpeg(inputPath)
      .videoCodec('libx264')
      .audioCodec('aac')
      .outputOptions([
        `-vf scale=-2:${quality.height}`,
        '-preset medium', // Better quality-to-compression ratio than 'fast'
        `-crf ${quality.crf}`,
        '-movflags +faststart' // Enables streaming playback
      ])
      .on('progress', (progress) => {
        if (progress?.percent) {
          console.log(`Transcoding ${quality.height}p: ${Math.round(progress.percent)}% done`);
        }
      })
      .on('end', () => {
        console.log(`Completed ${quality.height}p transcode`);
        command.kill('SIGTERM');
        resolve();
      })
      .on('error', (err) => {
        console.error(`Error in ${quality.height}p transcode:`, err);
        command.kill('SIGKILL');
        reject(err);
      });

    const timeout = setTimeout(() => {
      command.kill('SIGTERM');
      setTimeout(() => command.kill('SIGKILL'), 5000);
      reject(new Error(`FFmpeg process timed out for ${quality.height}p`));
    }, PROCESSING_TIMEOUT);

    command.save(outputPath)
      .on('end', () => clearTimeout(timeout));
  });
}

// Helper function to validate video metadata
async function validateVideo(filePath: string): Promise<VideoMetadata> {
  return new Promise((resolve, reject) => {
    ffmpeg.ffprobe(filePath, (err, metadata) => {
      if (err) {
        reject(new Error(`Failed to probe video file: ${err.message}`));
        return;
      }

      const format = metadata.format?.format_name?.toLowerCase() || '';
      if (!SUPPORTED_FORMATS.some(supported => format.includes(supported))) {
        reject(new Error(`Unsupported video format: ${format}`));
        return;
      }

      const videoStream = metadata.streams?.find(s => s.codec_type === 'video');
      if (!videoStream) {
        reject(new Error('No video stream found'));
        return;
      }

      resolve({
        duration: metadata.format?.duration,
        format: format,
        width: videoStream.width,
        height: videoStream.height
      });
    });
  });
}

// Health check endpoint
app.get('/', (req: Request, res: Response) => {
  res.send('Video processing service is operational');
});

// Main video processing endpoint
app.post('/transcode', async (req: Request, res: Response) => {
  const processingId = `process_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  const tempFiles: string[] = [];
  let data: ProcessingJob | undefined;
  
  // Cleanup helper
  const cleanup = () => {
    tempFiles.forEach(file => {
      try {
        if (fs.existsSync(file)) {
          fs.unlinkSync(file);
          console.log(`Cleaned up temporary file: ${file}`);
        }
      } catch (err) {
        console.error(`Failed to delete temp file ${file}:`, err);
      }
    });
  };

  try {
    console.log(`Starting job ${processingId}`);
    console.log('Received request body:', JSON.stringify(req.body, null, 2));

    // Enhanced Pub/Sub message validation
    if (!req.body?.message) {
      throw new Error('Invalid request: missing message object');
    }

    const pubSubMessage = req.body.message;
    if (!pubSubMessage.data) {
      throw new Error('Invalid Pub/Sub message: missing data field');
    }

    try {
      const decodedData = Buffer.from(pubSubMessage.data, 'base64').toString();
      console.log('Decoded message data:', decodedData);
      data = JSON.parse(decodedData);
    } catch (parseError) {
      throw new Error(`Failed to parse message data: ${(parseError as Error).message}`);
    }

    // Validate all required fields
    if (!data?.videoId) {
      throw new Error('Missing videoId in message data');
    }
    if (!data.bucket) {
      throw new Error('Missing bucket in message data');
    }
    if (!data.name) {
      throw new Error('Missing name in message data');
    }

    console.log('Validated processing job:', {
      videoId: data.videoId,
      bucket: data.bucket,
      name: data.name
    });

    // Check if video is already processed
    const docRef = db.collection('videos').doc(data.videoId);
    const doc = await docRef.get();
    if (doc.exists && doc.data()?.status === 'TRANSCODED') {
      console.log(`Video ${data.videoId} already processed, skipping`);
      return res.status(200).json({ message: 'Already processed' });
    }

    // Update initial status
    await docRef.set({
      status: 'PROCESSING',
      startedAt: admin.firestore.FieldValue.serverTimestamp(),
      processingId
    }, { merge: true });

    // Setup temporary file paths
    const tempInputFile = path.join('/tmp', `${data.videoId}_input.mp4`);
    tempFiles.push(tempInputFile);

    // Download source file
    console.log(`Downloading source file from gs://${data.bucket}/${data.name}`);
    await storage.bucket(data.bucket).file(data.name).download({ destination: tempInputFile });

    // Validate video
    const metadata = await validateVideo(tempInputFile);
    console.log('Video metadata:', metadata);

    // Process each quality
    const processedFiles: Record<string, string> = {};
    for (const [quality, settings] of Object.entries(VIDEO_QUALITIES)) {
      const tempOutputFile = path.join('/tmp', `${data.videoId}_${quality}.mp4`);
      tempFiles.push(tempOutputFile);

      // Skip transcoding if source is lower quality
      if (metadata.height && metadata.height < settings.height) {
        console.log(`Source height ${metadata.height} < target ${settings.height}, skipping ${quality}`);
        continue;
      }

      await transcodeVideo(tempInputFile, tempOutputFile, settings);

      // Upload to GCS
      const destination = `processed/${data.videoId}/${quality}.mp4`;
      await storage.bucket(BUCKET_NAME).upload(tempOutputFile, { destination });
      
      processedFiles[quality] = `gs://${BUCKET_NAME}/${destination}`;
    }

    // Update Firestore with results
    await docRef.update({
      status: 'TRANSCODED',
      processedFiles,
      metadata,
      completedAt: admin.firestore.FieldValue.serverTimestamp()
    });

    console.log(`Successfully processed video ${data.videoId}`);
    cleanup();
    return res.status(200).json({ message: 'Processing completed successfully' });

  } catch (error: any) {
    console.error('Processing error:', error);

    // Try to update Firestore with error status
    try {
      if (data?.videoId) {
        await db.collection('videos').doc(data.videoId).update({
          status: 'ERROR',
          error: error.message,
          updatedAt: admin.firestore.FieldValue.serverTimestamp()
        });
      }
    } catch (dbError) {
      console.error('Failed to update error status:', dbError);
    }

    cleanup();
    return res.status(500).json({ error: error.message });
  }
});

// Start server
const port = process.env.PORT || 8080;
app.listen(port, () => {
  console.log(`Video processing service running on port ${port}`);
});

export default app;