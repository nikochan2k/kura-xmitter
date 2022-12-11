import { FileSystemAsync, IdbLocalFileSystemAsync } from "kura";

export async function getFileSystem(purge = true): Promise<FileSystemAsync> {
  const idbFileSystem = new IdbLocalFileSystemAsync("web-file-system-test", {
    index: true,
  });
  const fs = await idbFileSystem.requestFileSystemAsync(
    window.PERSISTENT,
    Number.MAX_VALUE
  );
  if (purge) {
    await fs.filesystem.accessor.purge();
  }
  return fs;
}
