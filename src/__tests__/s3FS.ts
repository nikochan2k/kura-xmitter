import { FileSystemAsync } from "kura";
import { S3 } from "aws-sdk";
import { S3LocalFileSystemAsync } from "kura-s3";

export async function getFileSystem(purge = true): Promise<FileSystemAsync> {
  const options: S3.ClientConfiguration = {
    accessKeyId: "minioadmin",
    secretAccessKey: "minioadmin",
    endpoint: "http://127.0.0.1:9000",
    s3ForcePathStyle: true, // needed with minio?
    signatureVersion: "v4",
  };
  const s3 = new S3(options);
  const bucket = "web-file-system-test";
  try {
    await s3.createBucket({ Bucket: bucket }).promise();
  } catch (e) {}
  const s3FileSystem = new S3LocalFileSystemAsync(
    options,
    "web-file-system-test",
    "example",
    { index: true, noCache: true }
  );
  const fs = await s3FileSystem.requestFileSystemAsync(
    window.PERSISTENT,
    Number.MAX_VALUE
  );
  if (purge) {
    await fs.filesystem.accessor.purge();
  }
  return fs;
}
