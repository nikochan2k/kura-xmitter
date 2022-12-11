import { DIR_SEPARATOR, FileSystemAsync } from "kura";
import { NodeLocalFileSystemAsync } from "kura-node";
import { tmpdir } from "os";
import { normalize } from "path";

export async function getFileSystem(purge = true): Promise<FileSystemAsync> {
  const tempDir = tmpdir();
  let rootDir = `${tempDir}${DIR_SEPARATOR}web-file-system-index-test`;
  rootDir = normalize(rootDir);
  const nodeFileSystem = new NodeLocalFileSystemAsync(rootDir, {
    index: true,
  });
  const fs = await nodeFileSystem.requestFileSystemAsync(
    window.PERSISTENT,
    Number.MAX_VALUE
  );
  if (purge) {
    try {
      await fs.filesystem.accessor.purge();
    } catch (e) {
      console.warn(e);
    }
  }

  return fs;
}
