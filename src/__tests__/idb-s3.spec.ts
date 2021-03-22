require("fake-indexeddb/auto");
import { S3 } from "aws-sdk";
import { IdbLocalFileSystemAsync } from "kura";
import { S3LocalFileSystemAsync } from "kura-s3";
import { testAll } from "./syncronize";

testAll(async () => {
  const idbFileSystem = new IdbLocalFileSystemAsync("web-file-system-test", {
    index: true,
  });
  const local = await idbFileSystem.requestFileSystemAsync(
    window.PERSISTENT,
    Number.MAX_VALUE
  );
  await local.filesystem.accessor.purge();

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
  const remote = await s3FileSystem.requestFileSystemAsync(
    window.PERSISTENT,
    Number.MAX_VALUE
  );
  await remote.filesystem.accessor.purge();

  return { local, remote };
});
