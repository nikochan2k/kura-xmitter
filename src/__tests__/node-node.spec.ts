import { rmdirSync, statSync } from "fs";
import { DIR_SEPARATOR } from "kura";
import { NodeLocalFileSystemAsync, NodeTransferer } from "kura-node";
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

  const nodeFileSystem = new NodeLocalFileSystemAsync(rootDir, {
    index: true,
  });
  const local = await nodeFileSystem.requestFileSystemAsync(
    window.PERSISTENT,
    Number.MAX_VALUE
  );
  await local.filesystem.accessor.purge();

  let rootDir2 = `${tempDir}${DIR_SEPARATOR}web-file-system-index-test2`;
  rootDir2 = normalize(rootDir2);
  try {
    statSync(rootDir2);
    rmdirSync(rootDir2, { recursive: true });
  } catch {}

  const nodeFileSystem2 = new NodeLocalFileSystemAsync(rootDir2, {
    index: true,
  });
  const remote = await nodeFileSystem2.requestFileSystemAsync(
    window.PERSISTENT,
    Number.MAX_VALUE
  );
  await remote.filesystem.accessor.purge();

  const transferer = new NodeTransferer();

  return { local, remote, transferer };
});
