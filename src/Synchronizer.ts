import {
  AbstractAccessor,
  AbstractFileSystem,
  FileNameIndex,
  FileSystemAsync,
  FileSystemObject,
  getName,
  getParentPath,
  INDEX_FILE_PATH
} from "kura";

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
    await this.synchronizeDirectory(this.src.root.fullPath, true);
  }

  async synchronizeDirectory(dirPath: string, recursive: boolean) {
    await this.synchronize(
      dirPath,
      recursive,
      this.srcAccessor,
      this.dstAccessor
    );
  }

  private async copyFile(
    fromAccessor: AbstractAccessor,
    toAccessor: AbstractAccessor,
    obj: FileSystemObject
  ) {
    const srcBlob = await fromAccessor.getContent(obj.fullPath);
    await toAccessor.putObject(obj, srcBlob);
  }

  private async getIndex(
    accessor: AbstractAccessor,
    fullPath: string
  ): Promise<[FileNameIndex, string]> {
    const parentPath = getParentPath(fullPath);
    const name = getName(fullPath);
    const fileNameIndex = await accessor.getFileNameIndex(parentPath);
    return [fileNameIndex, name];
  }

  private async synchronize(
    dirPath: string,
    recursive: boolean,
    fromAccessor: AbstractAccessor,
    toAccessor: AbstractAccessor
  ) {
    // await this.synchronizeItself(dirPath, fromAccessor, toAccessor);

    const fileNameIndex: FileNameIndex = {};

    const fromFileNameIndex =
      (await fromAccessor.getFileNameIndex(dirPath)) || {};
    const toFileNameIndex = (await toAccessor.getFileNameIndex(dirPath)) || {};

    const fromNames = Object.keys(fromFileNameIndex);
    const toNames = Object.keys(toFileNameIndex);
    outer: while (0 < fromNames.length) {
      const srcName = fromNames.shift();
      if (!srcName) {
        break;
      }

      // source to destination
      for (let i = 0, end = toNames.length; i < end; i++) {
        const dstName = toNames[i];
        if (srcName !== dstName) {
          continue;
        }

        await this.synchronizeOne(
          recursive,
          fileNameIndex,
          fromAccessor,
          fromFileNameIndex,
          toAccessor,
          toFileNameIndex,
          srcName
        );

        toNames.splice(i);
        delete toFileNameIndex[dstName];
        continue outer;
      }

      // destination not found.
      await this.synchronizeOne(
        recursive,
        fileNameIndex,
        fromAccessor,
        fromFileNameIndex,
        toAccessor,
        toFileNameIndex,
        srcName
      );
    }

    // source not found
    for (const toRecord of Object.values(toFileNameIndex)) {
      const dstObj = toRecord.obj;
      const dstName = dstObj.name;
      await this.synchronizeOne(
        recursive,
        fileNameIndex,
        toAccessor,
        toFileNameIndex,
        fromAccessor,
        fromFileNameIndex,
        dstName
      );

      fileNameIndex[dstName] = toRecord;
    }

    console.log(dirPath, fileNameIndex);

    await fromAccessor.putFileNameIndex(dirPath, fileNameIndex);
    await toAccessor.putFileNameIndex(dirPath, fileNameIndex);
  }

  private async synchronizeItself(
    dirPath: string,
    fromAccessor: AbstractAccessor,
    toAccessor: AbstractAccessor
  ) {
    if (dirPath === "/") {
      return;
    }

    const [fromFileNameIndex, fromName] = await this.getIndex(
      fromAccessor,
      dirPath
    );
    const [toFileNameIndex] = await this.getIndex(toAccessor, dirPath);
    const fileNameIndex: FileNameIndex = {};
    await this.synchronizeOne(
      false,
      fileNameIndex,
      fromAccessor,
      fromFileNameIndex,
      toAccessor,
      toFileNameIndex,
      fromName
    );

    for (let [name, record] of Object.entries(fileNameIndex)) {
      fromFileNameIndex[name] = record;
      toFileNameIndex[name] = record;
    }

    await fromAccessor.putFileNameIndex(dirPath, fileNameIndex);
    await toAccessor.putFileNameIndex(dirPath, fileNameIndex);
  }

  private async synchronizeOne(
    recursive: boolean,
    fileNameIndex: FileNameIndex,
    fromAccessor: AbstractAccessor,
    fromFileNameIndex: FileNameIndex,
    toAccessor: AbstractAccessor,
    toFileNameIndex: FileNameIndex,
    name: string
  ) {
    let fromRecord = fromFileNameIndex[name];
    let toRecord = toFileNameIndex[name];

    let fromObj: FileSystemObject;
    let toObj: FileSystemObject;
    if (fromRecord != null && toRecord == null) {
      fromObj = fromRecord.obj;
      toRecord = { ...fromRecord };
      delete toRecord.deleted;
      toRecord.updated = 0;
      toObj = toRecord.obj;
    } else if (fromRecord == null && toRecord != null) {
      toObj = toRecord.obj;
      fromRecord = { ...toRecord };
      delete fromRecord.deleted;
      fromRecord.updated = 0;
      fromObj = fromRecord.obj;
    } else {
      fromObj = fromRecord.obj;
      toObj = toRecord.obj;
    }
    const fromFullPath = fromObj.fullPath;
    const toFullPath = toObj.fullPath;
    const fromDeleted = fromRecord.deleted;
    const toDeleted = toRecord.deleted;
    const fromUpdated = fromRecord.updated;
    const toUpdated = toRecord.updated;

    if (fromObj.size == null && toObj.size != null) {
      // TODO
      throw new Error("source is directory and destination is file");
    } else if (fromObj.size != null && toObj.size == null) {
      // TODO
      throw new Error("source is file and destination is directory");
    }

    if (fromObj.size != null) {
      // file
      if (fromDeleted != null && toDeleted == null) {
        if (fromDeleted <= toUpdated) {
          await this.copyFile(toAccessor, fromAccessor, toObj);
          fileNameIndex[name] = toRecord;
        } else {
          await toAccessor.delete(toFullPath, true);
          fileNameIndex[name] = fromRecord;
        }
      } else if (fromDeleted == null && toDeleted != null) {
        if (toDeleted <= fromUpdated) {
          await this.copyFile(fromAccessor, toAccessor, fromObj);
          fileNameIndex[name] = fromRecord;
        } else {
          await toAccessor.delete(fromFullPath, true);
          fileNameIndex[name] = toRecord;
        }
      } else if (fromDeleted != null && toDeleted != null) {
        // prioritize old
        if (fromDeleted <= toDeleted) {
          fileNameIndex[name] = fromRecord;
        } else {
          fileNameIndex[name] = toRecord;
        }
      } else {
        if (toUpdated < fromUpdated) {
          await this.copyFile(fromAccessor, toAccessor, fromObj);
          fileNameIndex[name] = fromRecord;
        } else if (fromUpdated < toUpdated) {
          await this.copyFile(toAccessor, fromAccessor, toObj);
          fileNameIndex[name] = toRecord;
        } else {
          fileNameIndex[name] = fromRecord;
        }
      }
    } else {
      // directory
      if (fromDeleted != null && toDeleted == null) {
        if (fromDeleted < toUpdated) {
          await fromAccessor.putObject(toObj);
          if (recursive) {
            await this.synchronize(
              toFullPath,
              recursive,
              toAccessor,
              fromAccessor
            );
          }
          fileNameIndex[name] = toRecord;
        } else {
          await toAccessor.deleteRecursively(toFullPath);
          fileNameIndex[name] = fromRecord;
        }
      } else if (fromDeleted == null && toDeleted != null) {
        if (toDeleted < fromUpdated) {
          await toAccessor.putObject(toObj);
          if (recursive) {
            await this.synchronize(
              toFullPath,
              recursive,
              fromAccessor,
              toAccessor
            );
          }
          fileNameIndex[name] = fromRecord;
        } else {
          await fromAccessor.deleteRecursively(fromFullPath);
          fileNameIndex[name] = toRecord;
        }
      } else if (fromDeleted != null && toDeleted != null) {
        // prioritize old
        if (fromDeleted <= toDeleted) {
          fileNameIndex[name] = fromRecord;
        } else {
          fileNameIndex[name] = toRecord;
        }
      } else {
        if (recursive) {
          if (fromUpdated < toUpdated) {
            await this.synchronize(
              toFullPath,
              recursive,
              toAccessor,
              fromAccessor
            );
            fileNameIndex[name] = toRecord;
          } else {
            await this.synchronize(
              fromFullPath,
              recursive,
              fromAccessor,
              toAccessor
            );
            fileNameIndex[name] = fromRecord;
          }
        } else {
          if (fromUpdated < toUpdated) {
            await fromAccessor.putObject(toObj);
            fileNameIndex[name] = toRecord;
          } else {
            await toAccessor.putObject(fromObj);
            fileNameIndex[name] = fromRecord;
          }
        }
      }
    }
  }
}
