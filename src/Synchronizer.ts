import {
  AbstractAccessor,
  AbstractFileSystem,
  FileSystemAsync,
  FileSystemObject,
  INDEX_FILE_PATH,
  FileNameIndex
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
    const srcFS = src.filesystem as AbstractFileSystem<AbstractAccessor>;
    this.srcAccessor = srcFS.accessor;
    if (!this.srcAccessor || !this.srcAccessor.hasIndex) {
      throw new Error(
        `Source filesystem "${srcFS.name}" has no index "${INDEX_FILE_PATH}"`
      );
    }

    const dstFS = dst.filesystem as AbstractFileSystem<AbstractAccessor>;
    this.dstAccessor = dstFS.accessor;
    if (!this.dstAccessor || !this.dstAccessor.hasIndex) {
      throw new Error(
        `Destination filesystem "${dstFS.name}" has no index "${INDEX_FILE_PATH}"`
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
    const srcFileNameIndex =
      (await this.srcAccessor.getFileNameIndex(dirPath)) || {};
    const dstFileNameIndex =
      (await this.dstAccessor.getFileNameIndex(dirPath)) || {};
    const fileNameIndex: FileNameIndex = {};

    const srcNames = Object.keys(srcFileNameIndex);
    const dstNames = Object.keys(dstFileNameIndex);
    outer: while (0 < srcNames.length) {
      const srcName = srcNames.shift();
      if (!srcName) {
        break;
      }
      const srcRecord = srcFileNameIndex[srcName];
      const srcObj = srcRecord.obj;
      for (let i = 0, end = dstNames.length; i < end; i++) {
        const dstName = dstNames[i];
        const dstRecord = dstFileNameIndex[dstName];
        const dstObj = dstRecord.obj;
        if (srcName === dstName) {
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
              fileNameIndex[srcName] = srcRecord;
            } else if (srcDeleted == null && dstDeleted != null) {
              deletion.srcFiles.push(srcObj.fullPath);
              fileNameIndex[dstName] = dstRecord;
            } else if (srcDeleted != null && dstDeleted != null) {
              if (srcDeleted <= dstDeleted) {
                fileNameIndex[srcName] = srcRecord;
              } else {
                fileNameIndex[dstName] = dstRecord;
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
                fileNameIndex[srcName] = srcRecord;
              } else {
                if (dstUpdated < srcUpdated) {
                  await this.copyFile(
                    this.srcAccessor,
                    this.dstAccessor,
                    srcObj
                  );
                  fileNameIndex[srcName] = srcRecord;
                } else {
                  await this.copyFile(
                    this.dstAccessor,
                    this.srcAccessor,
                    dstObj
                  );
                  fileNameIndex[dstName] = dstRecord;
                }
              }
            }
          } else {
            if (srcDeleted != null && dstDeleted == null) {
              deletion.dstDirectories.push(dstObj.fullPath);
              fileNameIndex[srcName] = srcRecord;
            } else if (srcDeleted == null && dstDeleted != null) {
              deletion.srcDirectories.push(srcObj.fullPath);
              fileNameIndex[dstName] = dstRecord;
            } else if (srcDeleted != null && dstDeleted != null) {
              if (srcDeleted <= dstDeleted) {
                fileNameIndex[srcName] = srcRecord;
              } else {
                fileNameIndex[dstName] = dstRecord;
              }
            } else {
              await this.synchronize(srcObj.fullPath, deletion);
              fileNameIndex[srcName] = srcRecord;
            }
          }

          dstNames.splice(i);
          delete dstFileNameIndex[dstName];
          continue outer;
        }
      }

      if (srcObj.size != null) {
        await this.copyFile(this.srcAccessor, this.dstAccessor, srcObj);
      } else {
        await this.dstAccessor.putObject(srcObj);
        await this.synchronize(srcObj.fullPath, deletion);
      }
      fileNameIndex[srcName] = srcRecord;
    }

    for (const dstRecord of Object.values(dstFileNameIndex)) {
      const dstObj = dstRecord.obj;
      const destPath = dstObj.fullPath;
      if (dstObj.size != null) {
        await this.copyFile(this.dstAccessor, this.srcAccessor, dstObj);
      } else {
        await this.srcAccessor.putObject(dstObj);
        await this.synchronize(destPath, deletion);
      }
      fileNameIndex[dstObj.name] = dstRecord;
    }

    await this.srcAccessor.putFileNameIndex(dirPath, fileNameIndex);
    await this.dstAccessor.putFileNameIndex(dirPath, fileNameIndex);
  }
}
