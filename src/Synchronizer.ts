import {
  AbstractAccessor,
  AbstractFileSystem,
  FileSystemAsync,
  FileSystemIndex,
  FileSystemObject,
  INDEX_FILE_NAME
} from "kura";

class Deletion {
  dstDirectories: string[] = [];
  dstFiles: string[] = [];
  srcDirectories: string[] = [];
  srcFiles: string[] = [];
}

export class Synchronizer {
  private dstAccessor: AbstractAccessor;
  private srcAccessor: AbstractAccessor;

  constructor(public src: FileSystemAsync, public dst: FileSystemAsync) {
    const srcFS = src.fileSystem as AbstractFileSystem<AbstractAccessor>;
    this.srcAccessor = srcFS.accessor;
    if (!this.srcAccessor || !this.srcAccessor.hasIndex) {
      throw new Error(
        `Source filesystem "${srcFS.name}" has no index "${INDEX_FILE_NAME}"`
      );
    }

    const dstFS = dst.fileSystem as AbstractFileSystem<AbstractAccessor>;
    this.dstAccessor = dstFS.accessor;
    if (!this.dstAccessor || !this.dstAccessor.hasIndex) {
      throw new Error(
        `Destination filesystem "${dstFS.name}" has no index "${INDEX_FILE_NAME}"`
      );
    }
  }

  async synchronizeAll() {
    await this.synchronizeDirectory(this.src.root.fullPath);
  }

  async synchronizeDirectory(dirPath: string) {
    const deletion = new Deletion();
    await this.synchronize(dirPath, deletion);

    for (let i = deletion.srcFiles.length - 1; 0 <= i; i--) {
      await this.srcAccessor.delete(deletion.srcFiles[i], true);
    }
    for (let i = deletion.dstFiles.length - 1; 0 <= i; i--) {
      await this.srcAccessor.delete(deletion.dstFiles[i], true);
    }
    for (let i = deletion.srcDirectories.length - 1; 0 <= i; i--) {
      await this.srcAccessor.delete(deletion.srcDirectories[i], false);
    }
    for (let i = deletion.dstDirectories.length - 1; 0 <= i; i--) {
      await this.srcAccessor.delete(deletion.dstDirectories[i], true);
    }
  }

  private async copyFile(
    srcAccessor: AbstractAccessor,
    dstAccessor: AbstractAccessor,
    obj: FileSystemObject
  ) {
    const srcBlob = await srcAccessor.getContent(obj.fullPath);
    await dstAccessor.putObject(obj, srcBlob);
  }

  private async synchronize(dirPath: string, deletion: Deletion) {
    const srcIndex = (await this.srcAccessor.getIndex(dirPath)) || {};
    const dstIndex = (await this.dstAccessor.getIndex(dirPath)) || {};
    const index: FileSystemIndex = {};

    const srcKeys = Object.keys(srcIndex);
    const dstKeys = Object.keys(dstIndex);
    outer: while (0 < srcKeys.length) {
      const srcPath = srcKeys.shift();
      if (!srcPath) {
        break;
      }
      const srcRecord = srcIndex[srcPath];
      const srcObj = srcRecord.obj;
      for (let i = 0, end = dstKeys.length; i < end; i++) {
        const dstPath = dstKeys[i];
        const dstRecord = dstIndex[dstPath];
        const dstObj = dstRecord.obj;
        if (srcPath === dstPath) {
          if (srcObj.size == null && dstObj.size != null) {
            // TODO
            throw new Error("source is directory and destination is file");
          } else if (srcObj.size != null && dstObj.size == null) {
            // TODO
            throw new Error("source is file and destination is directory");
          }

          const srcDeleted = srcRecord.deleted;
          const dstDeleted = dstRecord.deleted;
          if (srcObj.size != null) {
            if (srcDeleted != null && dstDeleted == null) {
              deletion.dstFiles.push(dstObj.fullPath);
              index[srcPath] = srcRecord;
            } else if (srcDeleted == null && dstDeleted != null) {
              deletion.srcFiles.push(srcObj.fullPath);
              index[dstPath] = dstRecord;
            } else if (srcDeleted != null && dstDeleted != null) {
              if (srcDeleted <= dstDeleted) {
                index[srcPath] = srcRecord;
              } else {
                index[dstPath] = dstRecord;
              }
            } else {
              const srcUpdated = srcRecord.updated;
              const dstUpdated = dstRecord.updated;
              if (srcUpdated === dstUpdated) {
                if (srcObj.size !== dstObj.size) {
                  await this.copyFile(
                    this.srcAccessor,
                    this.dstAccessor,
                    srcObj
                  );
                }
                index[srcPath] = srcRecord;
              } else {
                if (dstUpdated < srcUpdated) {
                  await this.copyFile(
                    this.srcAccessor,
                    this.dstAccessor,
                    srcObj
                  );
                  index[srcPath] = srcRecord;
                } else {
                  await this.copyFile(
                    this.dstAccessor,
                    this.srcAccessor,
                    dstObj
                  );
                  index[dstPath] = dstRecord;
                }
              }
            }
          } else {
            if (srcDeleted != null && dstDeleted == null) {
              deletion.dstDirectories.push(dstObj.fullPath);
              index[srcPath] = srcRecord;
            } else if (srcDeleted == null && dstDeleted != null) {
              deletion.srcDirectories.push(srcObj.fullPath);
              index[dstPath] = dstRecord;
            } else if (srcDeleted != null && dstDeleted != null) {
              if (srcDeleted <= dstDeleted) {
                index[srcPath] = srcRecord;
              } else {
                index[dstPath] = dstRecord;
              }
            } else {
              await this.synchronize(srcObj.fullPath, deletion);
              index[srcPath] = srcRecord;
            }
          }

          dstKeys.splice(i);
          delete dstIndex[dstPath];
          continue outer;
        }
      }

      if (srcObj.size != null) {
        await this.copyFile(this.srcAccessor, this.dstAccessor, srcObj);
      } else {
        await this.dstAccessor.putObject(srcObj);
        await this.synchronize(srcObj.fullPath, deletion);
      }
      index[srcPath] = srcRecord;
    }

    for (const dstRecord of Object.values(dstIndex)) {
      const dstObj = dstRecord.obj;
      const destPath = dstObj.fullPath;
      if (dstObj.size != null) {
        await this.copyFile(this.dstAccessor, this.srcAccessor, dstObj);
      } else {
        await this.srcAccessor.putObject(dstObj);
        await this.synchronize(destPath, deletion);
      }
      index[destPath] = dstRecord;
    }

    await this.srcAccessor.putIndex(dirPath, index);
    await this.dstAccessor.putIndex(dirPath, index);
  }
}
