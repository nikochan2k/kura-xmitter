import { S3 } from "aws-sdk";
import { rmdirSync, statSync } from "fs";
import { DIR_SEPARATOR, FileSystemAsync, NotFoundError } from "kura";
import { NodeLocalFileSystemAsync } from "kura-node";
import { S3LocalFileSystemAsync } from "kura-s3";
import { tmpdir } from "os";
import { normalize } from "path";
import { Synchronizer } from "../Synchronizer";

let local: FileSystemAsync;
let remote: FileSystemAsync;
let synchronizer: Synchronizer;
beforeAll(async () => {
  const tempDir = tmpdir();
  let rootDir = `${tempDir}${DIR_SEPARATOR}web-file-system-index-test`;
  rootDir = normalize(rootDir);
  try {
    statSync(rootDir);
    rmdirSync(rootDir, { recursive: true });
  } catch {}
  const nodeLocalFileSystem = new NodeLocalFileSystemAsync(rootDir, {
    useIndex: true,
    indexWriteDelayMillis: 0,
  });
  local = await nodeLocalFileSystem.requestFileSystemAsync(
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
    { useIndex: true, indexWriteDelayMillis: 0 }
  );
  remote = await s3LocalFileSystem.requestFileSystemAsync(
    window.PERSISTENT,
    Number.MAX_VALUE
  );

  synchronizer = new Synchronizer(local, remote, { verbose: true });
});

test("add a empty file, sync all", async (done) => {
  let remoteFE = await remote.root.getFile("empty.txt", {
    create: true,
    exclusive: true,
  });
  await synchronizer.synchronizeAll();

  remoteFE = await remote.root.getFile("empty.txt");
  const localReader = local.root.createReader();
  const localEntries = await localReader.readEntries();
  expect(localEntries.length).toBe(1);
  const localE = localEntries[0];
  expect(localE.isFile).toBe(true);
  expect(localE.name).toBe(remoteFE.name);
  expect(localE.fullPath).toBe(remoteFE.fullPath);

  const remoteMeta = await remoteFE.getMetadata();
  const localMeta = await localE.getMetadata();
  expect(localMeta.size).toBe(remoteMeta.size);

  done();
});

test("add a text file, sync all", async (done) => {
  let remoteFE = await remote.root.getFile("test.txt", {
    create: true,
    exclusive: true,
  });
  const writer = await remoteFE.createWriter();
  await writer.writeFile(new Blob(["hoge"], { type: "text/plain" }));

  await synchronizer.synchronizeAll();

  const localReader = local.root.createReader();
  const localEntries = await localReader.readEntries();
  expect(localEntries.length).toBe(2);

  let entries = localEntries.filter((le) => {
    return le.name === "empty.txt";
  });
  expect(entries.length).toBe(1);
  const emptyTxt = entries[0];
  expect(emptyTxt.isFile).toBe(true);
  const emptyTxtMeta = await emptyTxt.getMetadata();
  expect(emptyTxtMeta.size).toBe(0);

  entries = localEntries.filter((le) => {
    return le.name === "test.txt";
  });
  expect(entries.length).toBe(1);
  const testTxt = entries[0];
  expect(testTxt.isFile).toBe(true);
  remoteFE = await remote.root.getFile("test.txt");
  const remoteFEMeta = await remoteFE.getMetadata();
  const testTxtMeta = await testTxt.getMetadata();
  expect(testTxtMeta.size).toBe(remoteFEMeta.size);

  done();
});

test("add a hidden file, sync all", async (done) => {
  await remote.root.getFile(".hidden", {
    create: true,
    exclusive: true,
  });

  await synchronizer.synchronizeAll();

  const localReader = local.root.createReader();
  const localEntries = await localReader.readEntries();
  expect(localEntries.length).toBe(2);

  done();
});

test("create folder, and add a text file, sync all", async (done) => {
  let remoteDE = await remote.root.getDirectory("folder", {
    create: true,
    exclusive: true,
  });
  let remoteFE = await remoteDE.getFile("in.txt", {
    create: true,
    exclusive: true,
  });
  const writer = await remoteFE.createWriter();
  await writer.writeFile(new Blob(["hoge"], { type: "text/plain" }));

  await synchronizer.synchronizeAll();

  const remoteMeta = await remoteFE.getMetadata();
  const localDE = await local.root.getDirectory("folder");
  const localFE = await localDE.getFile("in.txt");
  const localMeta = await localFE.getMetadata();
  expect(localMeta.size).toBe(remoteMeta.size);

  done();
});

test("create nested folder, and add a empty file, sync dir", async (done) => {
  const remoteParentDE = await remote.root.getDirectory("folder");
  const remoteFE = await remoteParentDE.getFile("fuga.txt", {
    create: true,
    exclusive: true,
  });
  let writer = await remoteFE.createWriter();
  await writer.writeFile(new Blob(["fuga"], { type: "text/plain" }));

  const remoteDE = await remoteParentDE.getDirectory("nested", {
    create: true,
    exclusive: true,
  });
  const nestedFE = await remoteDE.getFile("nested.txt", {
    create: true,
    exclusive: true,
  });
  writer = await nestedFE.createWriter();
  await writer.writeFile(new Blob(["nested"], { type: "text/plain" }));

  await synchronizer.synchronizeDirectory("/folder", false);

  const remoteMeta = await remoteFE.getMetadata();
  const localParentDE = await local.root.getDirectory("folder");
  const localFE = await localParentDE.getFile("fuga.txt");
  const localMeta = await localFE.getMetadata();
  expect(localMeta.size).toBe(remoteMeta.size);
  const localDE = await localParentDE.getDirectory("nested");
  expect(localDE.fullPath).toBe(remoteDE.fullPath);
  try {
    await localDE.getFile("nested.txt");
  } catch (e) {
    expect(e).toBeInstanceOf(NotFoundError);
  }

  done();
});

test("sync dir recursively", async (done) => {
  await synchronizer.synchronizeDirectory("/folder", true);

  const remoteNestedFE = await remote.root.getFile("/folder/nested/nested.txt");
  const remoteNestedMeta = await remoteNestedFE.getMetadata();
  const localParentDE = await local.root.getDirectory("folder");
  const localDE = await localParentDE.getDirectory("nested");
  const localNestedFE = await localDE.getFile("nested.txt");
  const localNestedMeta = await localNestedFE.getMetadata();
  expect(localNestedMeta.size).toBe(remoteNestedMeta.size);

  done();
});

test("remove file, sync all", async (done) => {
  let remoteFE = await remote.root.getFile("empty.txt");
  await remoteFE.remove();

  await synchronizer.synchronizeAll();

  const localFE = await local.root.getFile("/empty.txt");
  expect(localFE).toBeNull();

  done();
});
