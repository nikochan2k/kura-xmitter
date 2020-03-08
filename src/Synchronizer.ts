import {
  AbstractAccessor,
  AbstractFileSystem,
  DirPathIndex,
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

  constructor(
    public src: FileSystemAsync,
    public dst: FileSystemAsync,
    private verbose = false
  ) {
    const srcFS = src.filesystem as AbstractFileSystem<AbstractAccessor>;
    this.srcAccessor = srcFS.accessor;
    if (!this.srcAccessor || !this.srcAccessor.options.useIndex) {
      throw new Error(
        `Source filesystem "${srcFS.name}" has no index "${INDEX_FILE_PATH}"`
      );
    }

    const dstFS = dst.filesystem as AbstractFileSystem<AbstractAccessor>;
    this.dstAccessor = dstFS.accessor;
    if (!this.dstAccessor || !this.dstAccessor.options.useIndex) {
      throw new Error(
        `Destination filesystem "${dstFS.name}" has no index "${INDEX_FILE_PATH}"`
      );
    }
  }

  async synchronizeAll() {
    await this.synchronizeDirectory(this.src.root.fullPath, true);
  }

  async synchronizeDirectory(dirPath: string, recursive: boolean) {
    if (this.srcAccessor.options.verbose || this.dstAccessor.options.verbose) {
      console.log(`synchronize ${dirPath}`);
    }

    const srcDirPathIndex = await this.srcAccessor.getDirPathIndex();
    const dstDirPathIndex = await this.dstAccessor.getDirPathIndex();

    await this.synchronizeSelf(dirPath);

    await this.synchronize(
      dirPath,
      recursive,
      this.srcAccessor,
      srcDirPathIndex,
      this.dstAccessor,
      dstDirPathIndex
    );

    await this.srcAccessor.putDirPathIndex(srcDirPathIndex);
    await this.dstAccessor.putDirPathIndex(dstDirPathIndex);
  }

  private async copyFile(
    fromAccessor: AbstractAccessor,
    toAccessor: AbstractAccessor,
    obj: FileSystemObject
  ) {
    this.debug(fromAccessor, toAccessor, "copyFile", obj.fullPath);
    const blob = await fromAccessor._getContent(obj.fullPath);
    await toAccessor._putObject(obj);
    await toAccessor._putContent(obj.fullPath, blob);
  }

  private debug(
    fromAccessor: AbstractAccessor,
    toAccessor: AbstractAccessor,
    title: string,
    path: string
  ) {
    if (!this.verbose) {
      return;
    }
    if (fromAccessor) {
      console.log(
        `${fromAccessor.name} => ${toAccessor.name} - ${title}: ${path}`
      );
    } else {
      console.log(`${toAccessor.name} - ${title}: ${path}`);
    }
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
    fromDirPathIndex: DirPathIndex,
    toAccessor: AbstractAccessor,
    toDirPathIndex: DirPathIndex
  ) {
    let fromFileNameIndex = fromDirPathIndex[dirPath];
    if (!fromFileNameIndex) {
      fromFileNameIndex = {};
      fromDirPathIndex[dirPath] = fromFileNameIndex;
    }
    let toFileNameIndex = toDirPathIndex[dirPath];
    if (!toFileNameIndex) {
      toFileNameIndex = {};
      toDirPathIndex[dirPath] = toFileNameIndex;
    }

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
          fromAccessor,
          fromDirPathIndex,
          fromFileNameIndex,
          toAccessor,
          toDirPathIndex,
          toFileNameIndex,
          srcName
        );

        toNames.splice(i);
        continue outer;
      }

      // destination not found.
      await this.synchronizeOne(
        recursive,
        fromAccessor,
        fromDirPathIndex,
        fromFileNameIndex,
        toAccessor,
        toDirPathIndex,
        toFileNameIndex,
        srcName
      );
    }

    // source not found
    for (const toName of toNames) {
      await this.synchronizeOne(
        recursive,
        toAccessor,
        toDirPathIndex,
        toFileNameIndex,
        fromAccessor,
        fromDirPathIndex,
        fromFileNameIndex,
        toName
      );
    }
  }

  private async synchronizeOne(
    recursive: boolean,
    fromAccessor: AbstractAccessor,
    fromDirPathIndex: DirPathIndex,
    fromFileNameIndex: FileNameIndex,
    toAccessor: AbstractAccessor,
    toDirPathIndex: DirPathIndex,
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
          fromFileNameIndex[name] = toRecord;
        } else {
          this.debug(null, toAccessor, "delete", toFullPath);
          await toAccessor._delete(toFullPath, true);
          toFileNameIndex[name] = fromRecord;
        }
      } else if (fromDeleted == null && toDeleted != null) {
        if (toDeleted <= fromUpdated) {
          await this.copyFile(fromAccessor, toAccessor, fromObj);
          toFileNameIndex[name] = fromRecord;
        } else {
          this.debug(null, fromAccessor, "delete", fromFullPath);
          await fromAccessor._delete(fromFullPath, true);
          fromFileNameIndex[name] = toRecord;
        }
      } else if (fromDeleted != null && toDeleted != null) {
        // prioritize old
        if (fromDeleted < toDeleted) {
          toFileNameIndex[name] = fromRecord;
        } else if (toDeleted < fromDeleted) {
          fromFileNameIndex[name] = toRecord;
        }
      } else {
        if (toUpdated < fromUpdated) {
          await this.copyFile(fromAccessor, toAccessor, fromObj);
          toFileNameIndex[name] = fromRecord;
        } else if (fromUpdated < toUpdated) {
          await this.copyFile(toAccessor, fromAccessor, toObj);
          fromFileNameIndex[name] = toRecord;
        } else {
          if (fromObj.size !== toObj.size) {
            await this.copyFile(fromAccessor, toAccessor, fromObj);
            toFileNameIndex[name] = fromRecord;
          }
        }
      }
    } else {
      // directory
      if (fromDeleted != null && toDeleted == null) {
        if (fromDeleted <= toUpdated) {
          this.debug(null, fromAccessor, "putObject", toFullPath);
          await fromAccessor._putObject(toObj);
          if (recursive) {
            await this.synchronize(
              toFullPath,
              recursive,
              toAccessor,
              toDirPathIndex,
              fromAccessor,
              fromDirPathIndex
            );
          }
          fromFileNameIndex[name] = toRecord;
        } else {
          await this.synchronize(
            toFullPath,
            recursive,
            toAccessor,
            toDirPathIndex,
            fromAccessor,
            fromDirPathIndex
          );
          this.debug(null, toAccessor, "delete", toFullPath);
          await toAccessor._delete(fromFullPath, false);
          toFileNameIndex[name] = fromRecord;
        }
      } else if (fromDeleted == null && toDeleted != null) {
        if (toDeleted <= fromUpdated) {
          this.debug(null, toAccessor, "putObject", toFullPath);
          await toAccessor._putObject(fromObj);
          if (recursive) {
            await this.synchronize(
              toFullPath,
              recursive,
              fromAccessor,
              fromDirPathIndex,
              toAccessor,
              toDirPathIndex
            );
          }
          toFileNameIndex[name] = fromRecord;
        } else {
          await this.synchronize(
            toFullPath,
            recursive,
            fromAccessor,
            fromDirPathIndex,
            toAccessor,
            toDirPathIndex
          );
          this.debug(null, fromAccessor, "delete", fromFullPath);
          await fromAccessor._delete(toFullPath, false);
          fromFileNameIndex[name] = toRecord;
        }
      } else if (fromDeleted != null && toDeleted != null) {
        // prioritize old
        if (fromDeleted < toDeleted) {
          toFileNameIndex[name] = fromRecord;
        } else if (toDeleted < fromDeleted) {
          fromFileNameIndex[name] = toRecord;
        }
      } else {
        if (fromUpdated < toUpdated) {
          this.debug(null, fromAccessor, "putObject", toFullPath);
          await fromAccessor._putObject(toObj);
          fromFileNameIndex[name] = toRecord;
        } else if (toUpdated < fromUpdated) {
          this.debug(null, toAccessor, "putObject", fromFullPath);
          await toAccessor._putObject(fromObj);
          toFileNameIndex[name] = fromRecord;
        }
        if (recursive) {
          await this.synchronize(
            fromFullPath,
            recursive,
            fromAccessor,
            fromDirPathIndex,
            toAccessor,
            toDirPathIndex
          );
        }
      }
    }
  }

  private async synchronizeSelf(dirPath: string) {
    if (dirPath === "/") {
      return;
    }

    const [srcFileNameIndex, name] = await this.getIndex(
      this.srcAccessor,
      dirPath
    );
    const srcRecord = srcFileNameIndex[name];
    const srcObj = srcRecord.obj;
    const [dstFileNameIndex] = await this.getIndex(this.dstAccessor, dirPath);
    const dstRecord = dstFileNameIndex[name];
    if (dstRecord == null) {
      await this.copyFile(this.srcAccessor, this.dstAccessor, srcObj);
      dstFileNameIndex[name] = srcRecord;
    } else if (srcRecord.updated < dstRecord.deleted) {
      this.debug(null, this.srcAccessor, "delete", srcRecord.obj.fullPath);
      await this.srcAccessor._delete(srcRecord.obj.fullPath, false); // TODO  recurcively
    }
  }
}
