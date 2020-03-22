require("fake-indexeddb/auto");
import { S3 } from "aws-sdk";
import {
  AbstractAccessor,
  FileSystemAsync,
  IdbLocalFileSystemAsync,
  NotFoundError
} from "kura";
import { S3LocalFileSystemAsync } from "kura-s3";
import { Synchronizer } from "../Synchronizer";

AbstractAccessor.PUT_INDEX_THROTTLE = 0;

let local: FileSystemAsync;
let remote: FileSystemAsync;
let synchronizer: Synchronizer;
beforeAll(async () => {
  const idbLocalFileSystem = new IdbLocalFileSystemAsync(
    "web-file-system-test",
    { useIndex: true }
  );
  local = await idbLocalFileSystem.requestFileSystemAsync(
    window.PERSISTENT,
    Number.MAX_VALUE
  );

  const options: S3.ClientConfiguration = {
    accessKeyId: "KFS0LZVKZ8G456A502L3",
    secretAccessKey: "uVwBONMdTwJI1+C8jUhrypvshHz3OY8Ooar3amdC",
    endpoint: "http://127.0.0.1:9000",
    s3ForcePathStyle: true, // needed with minio?
    signatureVersion: "v4"
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
    { useIndex: true }
  );
  remote = await s3LocalFileSystem.requestFileSystemAsync(
    window.PERSISTENT,
    Number.MAX_VALUE
  );

  synchronizer = new Synchronizer(local, remote, { verbose: true });
});

test("add a empty file, sync all", async done => {
  let localFE = await local.root.getFile("empty.txt", {
    create: true,
    exclusive: true
  });
  await synchronizer.synchronizeAll();

  localFE = await local.root.getFile("empty.txt");
  const remoteReader = remote.root.createReader();
  const remoteEntries = await remoteReader.readEntries();
  expect(remoteEntries.length).toBe(1);
  const remoteE = remoteEntries[0];
  expect(remoteE.isFile).toBe(true);
  expect(remoteE.name).toBe(localFE.name);
  expect(remoteE.fullPath).toBe(localFE.fullPath);

  const localMeta = await localFE.getMetadata();
  const remoteMeta = await remoteE.getMetadata();
  expect(remoteMeta.size).toBe(localMeta.size);

  done();
});

test("add a text file, sync all", async done => {
  let localFE = await local.root.getFile("test.txt", {
    create: true,
    exclusive: true
  });
  const writer = await localFE.createWriter();
  await writer.writeFile(new Blob(["hoge"], { type: "text/plain" }));

  await synchronizer.synchronizeAll();

  const remoteReader = remote.root.createReader();
  const remoteEntries = await remoteReader.readEntries();
  expect(remoteEntries.length).toBe(2);

  let entries = remoteEntries.filter(re => {
    return re.name === "empty.txt";
  });
  expect(entries.length).toBe(1);
  const emptyTxt = entries[0];
  expect(emptyTxt.isFile).toBe(true);
  const emptyTxtMeta = await emptyTxt.getMetadata();
  expect(emptyTxtMeta.size).toBe(0);

  entries = remoteEntries.filter(re => {
    return re.name === "test.txt";
  });
  expect(entries.length).toBe(1);
  const testTxt = entries[0];
  expect(testTxt.isFile).toBe(true);
  localFE = await local.root.getFile("test.txt");
  const localFEMeta = await localFE.getMetadata();
  const testTxtMeta = await testTxt.getMetadata();
  expect(testTxtMeta.size).toBe(localFEMeta.size);

  done();
});

test("add a hidden file, sync all", async done => {
  await local.root.getFile(".hidden", {
    create: true,
    exclusive: true
  });

  await synchronizer.synchronizeAll();

  const remoteReader = remote.root.createReader();
  const remoteEntries = await remoteReader.readEntries();
  expect(remoteEntries.length).toBe(2);

  done();
});

test("create folder, and add a text file, sync all", async done => {
  let localDE = await local.root.getDirectory("folder", {
    create: true,
    exclusive: true
  });
  let localFE = await localDE.getFile("in.txt", {
    create: true,
    exclusive: true
  });
  const writer = await localFE.createWriter();
  await writer.writeFile(new Blob(["hoge"], { type: "text/plain" }));

  await synchronizer.synchronizeAll();

  const localMeta = await localFE.getMetadata();
  const remoteDE = await remote.root.getDirectory("folder");
  const remoteFE = await remoteDE.getFile("in.txt");
  const remoteMeta = await remoteFE.getMetadata();
  expect(remoteMeta.size).toBe(localMeta.size);

  done();
});

test("create nested folder, and add a empty file, sync dir", async done => {
  const localParentDE = await local.root.getDirectory("folder");
  const localFE = await localParentDE.getFile("fuga.txt", {
    create: true,
    exclusive: true
  });
  let writer = await localFE.createWriter();
  await writer.writeFile(new Blob(["fuga"], { type: "text/plain" }));

  const localDE = await localParentDE.getDirectory("nested", {
    create: true,
    exclusive: true
  });
  const nestedFE = await localDE.getFile("nested.txt", {
    create: true,
    exclusive: true
  });
  writer = await nestedFE.createWriter();
  await writer.writeFile(new Blob(["nested"], { type: "text/plain" }));

  await synchronizer.synchronizeDirectory("/folder", false);

  const localMeta = await localFE.getMetadata();
  const remoteParentDE = await remote.root.getDirectory("folder");
  const remoteFE = await remoteParentDE.getFile("fuga.txt");
  const remoteMeta = await remoteFE.getMetadata();
  expect(remoteMeta.size).toBe(localMeta.size);
  const remoteDE = await remoteParentDE.getDirectory("nested");
  expect(remoteDE.fullPath).toBe(localDE.fullPath);
  try {
    await remoteDE.getFile("nested.txt");
  } catch (e) {
    expect(e).toBeInstanceOf(NotFoundError);
  }

  done();
});

test("sync dir recursively", async done => {
  await synchronizer.synchronizeDirectory("/folder", true);

  const localNestedFE = await local.root.getFile("/folder/nested/nested.txt");
  const localNestedMeta = await localNestedFE.getMetadata();
  const remoteParentDE = await remote.root.getDirectory("folder");
  const remoteDE = await remoteParentDE.getDirectory("nested");
  const remoteNestedFE = await remoteDE.getFile("nested.txt");
  const remoteNestedMeta = await remoteNestedFE.getMetadata();
  expect(remoteNestedMeta.size).toBe(localNestedMeta.size);

  done();
});

test("remove file, sync all", async done => {
  let localFE = await local.root.getFile("empty.txt");
  await localFE.remove();

  await synchronizer.synchronizeAll();

  const remoteFE = await remote.root.getFile("/empty.txt");
  expect(remoteFE).toBeNull();

  done();
});