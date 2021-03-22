import { S3 } from "aws-sdk";
import { rmdirSync, statSync } from "fs";
import { DIR_SEPARATOR } from "kura";
import { NodeLocalFileSystemAsync } from "kura-node";
import { S3LocalFileSystemAsync } from "kura-s3";
import { tmpdir } from "os";
import { normalize } from "path";
import { testAll } from "./syncronize";

testAll(async () => {
  const tempDir = tmpdir();
  let rootDir = `${tempDir}${DIR_SEPARATOR}web-file-system-index-test`;
  rootDir = normalize(rootDir);
  try {
    statSync(rootDir);
    rmdirSync(rootDir, { recursive: true });
  } catch {}

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
    { index: true }
  );
  const local = await s3FileSystem.requestFileSystemAsync(
    window.PERSISTENT,
    Number.MAX_VALUE
  );
  await local.filesystem.accessor.purge();

  const nodeFileSystem = new NodeLocalFileSystemAsync(rootDir, {
    index: true,
  });
  const remote = await nodeFileSystem.requestFileSystemAsync(
    window.PERSISTENT,
    Number.MAX_VALUE
  );
  await remote.filesystem.accessor.purge();

  return { local, remote };
});
