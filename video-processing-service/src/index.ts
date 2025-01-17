import express, { Express, Request, Response } from 'express';
import ffmpeg from 'fluent-ffmpeg';
import { Storage } from '@google-cloud/storage';
import admin from 'firebase-admin';
import path from 'path';
import fs from 'fs';

// 1. Initialize Firebase Admin for Firestore
//    In Cloud Run, you'll rely on the default service account or a custom service account
//    with permissions to access Firestore & GCS.
admin.initializeApp();
const db = admin.firestore();

// 2. Initialize GCS client
const storage = new Storage();

// 3. Create Express app
const app: Express = express();
app.use(express.json());

// Health check / root
app.get('/', (req: Request, res: Response) => {
  res.send('Cloud Run transcoding service is up my dude');
});

// 4. Pub/Sub push endpoint
//    Expects a JSON body: { videoId, rawFilePath }
//    The rawFilePath is something like gs://my-bucket/raw/video123.mp4
app.post('/transcode', async (req: Request, res: Response) => {
  // Add timeout tracking
  const startTime = Date.now();
  const MAX_PROCESSING_TIME = 540000; // 9 minutes (leaving buffer before Pub/Sub timeout)

  try {
    // Parse the Pub/Sub message
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
      
      const { bucket, name } = data;
      if (!bucket || !name) {
        console.error('Missing bucket or name in Pub/Sub message');
        return res.status(400).json({ error: 'Missing bucket or name in Pub/Sub message' });
      }
      console.log('Full request body:', JSON.stringify(req.body, null, 2));

      
    const videoId = path.parse(data.name).name; // Extract filename without extension
    const rawFilePath = `gs://${data.bucket}/${data.name}`;

    if (!videoId || !rawFilePath) {
      return res.status(400).json({ error: 'Missing videoId or rawFilePath' });
    }

    // Extract bucket name and file path from the "gs://bucketName/filename" string.
    const [bucketName, ...filePathArr] = rawFilePath.replace('gs://', '').split('/');
    const filePath = filePathArr.join('/');

    // Check processing time before each major operation
    if (Date.now() - startTime > MAX_PROCESSING_TIME) {
      throw new Error('Processing timeout exceeded');
    }

    // 5. Download raw video to ephemeral /tmp storage
    const tempInputFile = path.join('/tmp', `${videoId}_input.mp4`);
    await storage.bucket(bucketName).file(filePath).download({ destination: tempInputFile });
    console.log(`Downloaded raw video to ${tempInputFile}`);

    // Add after downloading the input file
    await new Promise<void>((resolve, reject) => {
      ffmpeg.ffprobe(tempInputFile, (err, metadata) => {
        if (err) reject(err);
        if (!metadata.streams?.length) {
          reject(new Error('Invalid video file'));
        }
        resolve();
      });
    });

    // We'll create two local output files for 360p and 720p
    const temp360File = path.join('/tmp', `${videoId}_360.mp4`);
    const temp720File = path.join('/tmp', `${videoId}_720.mp4`);

    // 6. Transcode to 360p
    await new Promise<void>((resolve, reject) => {
      ffmpeg(tempInputFile)
        .outputOptions('-vf', 'scale=-1:360')
        .on('end', () => resolve())
        .on('error', (err) => reject(err))
        .save(temp360File);
    });
    console.log('360p transcoding finished');

    // 7. Transcode to 720p
    await new Promise<void>((resolve, reject) => {
      ffmpeg(tempInputFile)
        .outputOptions('-vf', 'scale=-1:720')
        .on('end', () => resolve())
        .on('error', (err) => reject(err))
        .save(temp720File);
    });
    console.log('720p transcoding finished');

    // 8. Upload processed files to GCS
    const processed360 = `processed/${videoId}/360p.mp4`;
    const processed720 = `processed/${videoId}/720p.mp4`;

    await storage.bucket(bucketName).upload(temp360File, { destination: processed360 });
    await storage.bucket(bucketName).upload(temp720File, { destination: processed720 });
    console.log(`Uploaded 360p -> gs://${bucketName}/${processed360}`);
    console.log(`Uploaded 720p -> gs://${bucketName}/${processed720}`);

    // 9. Update Firestore metadata
    //    We'll assume there's a "videos" collection with docs keyed by "videoId".
    await db.collection('videos').doc(videoId).update({
      status: 'PROCESSING_360P',
      updatedAt: admin.firestore.FieldValue.serverTimestamp()
    });

    // After 360p complete
    await db.collection('videos').doc(videoId).update({
      status: 'PROCESSING_720P',
      processedFiles: {
        '360p': `gs://${bucketName}/${processed360}`
      },
      updatedAt: admin.firestore.FieldValue.serverTimestamp()
    });

    // 10. Clean up local temp files
    fs.unlinkSync(tempInputFile);
    fs.unlinkSync(temp360File);
    fs.unlinkSync(temp720File);

    // 11. Acknowledge the push request
    return res.status(200).json({ message: 'Transcoding completed successfully' });
  } catch (error: any) {
    // Add specific error handling for timeouts
    if (error.message === 'Processing timeout exceeded') {
      console.error('Processing timeout - message will be redelivered');
      return res.status(429).json({ error: 'Processing timeout' });
    }
    console.error('Error during transcoding:', error);
    return res.status(500).json({ error: error.message });
  }
});

// Start the server in dev mode (3000). In Cloud Run, we typically run on PORT=8080
const port = process.env.PORT || 8080;
app.listen(port, () => {
  console.log(`Video processing service running on port ${port}`);
});

export default app;
