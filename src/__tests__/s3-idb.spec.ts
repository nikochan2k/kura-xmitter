require("fake-indexeddb/auto");
import { S3 } from "aws-sdk";
import { IdbLocalFileSystemAsync } from "kura";
import { S3LocalFileSystemAsync } from "kura-s3";
import { testAll } from "./syncronize";

testAll(async () => {
  const idbLocalFileSystem = new IdbLocalFileSystemAsync(
    "web-file-system-test",
    { index: true }
  );
  const local = await idbLocalFileSystem.requestFileSystemAsync(
    window.PERSISTENT,
    Number.MAX_VALUE
  );

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
  const list = await s3.listObjectsV2({ Bucket: bucket }).promise();
  for (const content of list.Contents) {
    await s3.deleteObject({ Bucket: bucket, Key: content.Key }).promise();
  }

  const s3LocalFileSystem = new S3LocalFileSystemAsync(
    options,
    "web-file-system-test",
    "example",
    { index: true }
  );
  const remote = await s3LocalFileSystem.requestFileSystemAsync(
    window.PERSISTENT,
    Number.MAX_VALUE
  );

  return { local, remote };
});
