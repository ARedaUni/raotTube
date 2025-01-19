import express, { Express, Request, Response } from 'express';
import ffmpeg from 'fluent-ffmpeg';
import { Storage } from '@google-cloud/storage';
import admin from 'firebase-admin';
import path from 'path';
import fs from 'fs';

// 1. Initialize Firebase Admin (needed for Firestore updates)
admin.initializeApp();

// Firestore instance
const db = admin.firestore();

// 2. Initialize the Google Cloud Storage client
const storage = new Storage();

// 3. Create Express app
const app: Express = express();
app.use(express.json());

// Health check / root endpoint
app.get('/', (req: Request, res: Response) => {
  res.send('Cloud Run transcoding service is up and running');
});

/**
 * 4. Transcode Endpoint:
 *    - Triggered by a Pub/Sub push subscription (or direct HTTP).
 *    - Expects JSON containing at least { videoId, bucket, name }.
 *    - GCS input file => /tmp => transcode => new GCS objects => Firestore update.
 */
app.post('/transcode', async (req: Request, res: Response) => {
  try {
    // Extract Pub/Sub message or direct request fields
    // Example Pub/Sub push JSON: { "message": { "data": "<base64>" } }
    const message = req.body?.message;
    if (!message || !message.data) {
      console.error('Missing Pub/Sub message or data');
      return res.status(400).json({ error: 'Missing Pub/Sub message or data' });
    }

    let data;
    try {
      data = JSON.parse(Buffer.from(message.data, 'base64').toString());
    } catch (error) {
      console.error('Failed to decode or parse Pub/Sub message:', error);
      return res.status(400).json({ error: 'Invalid Pub/Sub message format' });
    }

    // data is expected to have: { videoId, bucket, name }
    const { videoId, bucket, name } = data;
    if (!videoId || !bucket || !name) {
      console.error('Missing required fields in message data');
      return res.status(400).json({ error: 'Missing videoId, bucket, or name' });
    }

    // The raw file path (e.g., gs://my-bucket/raw/video123.mp4)
    const rawFilePath = `gs://${bucket}/${name}`;
    console.log('Pub/Sub message data:', JSON.stringify(data, null, 2));
    console.log(`Raw file path: ${rawFilePath}`);

    // Parse bucketName and filePath from "gs://bucketName/filename"
    const [bucketName, ...filePathArr] = rawFilePath.replace('gs://', '').split('/');
    const filePath = filePathArr.join('/');

    // Prepare local paths in ephemeral /tmp
    const tempInputFile = path.join('/tmp', `${videoId}_input.mp4`);
    const temp360File = path.join('/tmp', `${videoId}_360.mp4`);
    const temp720File = path.join('/tmp', `${videoId}_720.mp4`);

    // Download raw input from GCS
    await storage.bucket(bucketName).file(filePath).download({ destination: tempInputFile });
    console.log(`Downloaded raw video to: ${tempInputFile}`);

    // Optional: Perform a quick ffprobe to validate the input
    await new Promise<void>((resolve, reject) => {
      ffmpeg.ffprobe(tempInputFile, (err, metadata) => {
        if (err) return reject(err);
        console.log('Input video metadata:', metadata.format);
        resolve();
      });
    });

    // 5. Transcode to 360p with explicit codecs
    await new Promise<void>((resolve, reject) => {
      ffmpeg(tempInputFile)
        .videoCodec('libx264')       // H.264 video
        .audioCodec('aac')          // AAC audio
        // Additional output options:
        .outputOptions([
          '-vf scale=-1:360',       // Scale video to 360p, keeping aspect ratio
          '-preset fast',           // Faster preset
          '-crf 23'                 // Constant Rate Factor for video quality
        ])
        .on('start', (cmdLine) => console.log('FFmpeg 360p command:', cmdLine))
        .on('end', () => {
          console.log('Transcoding to 360p finished');
          resolve();
        })
        .on('error', (err) => {
          console.error('Error transcoding to 360p:', err);
          reject(err);
        })
        .save(temp360File);
    });

    // 6. Transcode to 720p with explicit codecs
    await new Promise<void>((resolve, reject) => {
      ffmpeg(tempInputFile)
        .videoCodec('libx264')
        .audioCodec('aac')
        .outputOptions([
          '-vf scale=-1:720',
          '-preset fast',
          '-crf 23'
        ])
        .on('start', (cmdLine) => console.log('FFmpeg 720p command:', cmdLine))
        .on('end', () => {
          console.log('Transcoding to 720p finished');
          resolve();
        })
        .on('error', (err) => {
          console.error('Error transcoding to 720p:', err);
          reject(err);
        })
        .save(temp720File);
    });

    // 7. Upload processed files back to GCS
    //    NOTE: GCS automatically "creates" directories if they do not exist.
    //    We simply specify the destination path, and GCS handles the rest.
    const processed360 = `processed/${videoId}/360p.mp4`;
    const processed720 = `processed/${videoId}/720p.mp4`;

    await storage.bucket(bucketName).upload(temp360File, { destination: processed360 });
    console.log(`Uploaded 360p to: gs://${bucketName}/${processed360}`);

    await storage.bucket(bucketName).upload(temp720File, { destination: processed720 });
    console.log(`Uploaded 720p to: gs://${bucketName}/${processed720}`);

    // 8. Update Firestore doc (videos/{videoId})
    //    We'll mark that transcoding is done and store processed file paths
    await db.collection('videos').doc(videoId).update({
      status: 'TRANSCODED',
      processedFiles: {
        '360p': `gs://${bucketName}/${processed360}`,
        '720p': `gs://${bucketName}/${processed720}`
      },
      updatedAt: admin.firestore.FieldValue.serverTimestamp()
    });
    console.log(`Firestore updated for videoId=${videoId}`);

    // 9. Clean up local files
    fs.unlinkSync(tempInputFile);
    fs.unlinkSync(temp360File);
    fs.unlinkSync(temp720File);
    console.log('Local temporary files removed');

    return res.status(200).json({ message: 'Transcoding completed successfully' });
  } catch (error: any) {
    console.error('Error during transcoding:', error);
    return res.status(500).json({ error: error.message });
  }
});

// 10. Start server: In Cloud Run, the PORT is typically 8080
const port = process.env.PORT || 8080;
app.listen(port, () => {
  console.log(`Transcoding service running on port ${port}`);
});

export default app;
