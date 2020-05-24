import {
  AbstractAccessor,
  DirPathIndex,
  DIR_SEPARATOR,
  FileNameIndex,
  FileSystemAsync,
  FileSystemObject,
  getName,
  getParentPath,
  INDEX_FILE_PATH,
  NotFoundError,
  Record,
} from "kura";
import { SyncOptions } from "./SyncOptions";

export class Synchronizer {
  private static NOT_EXISTS = 0;

  private dstAccessor: AbstractAccessor;
  private excludeFileNameRegExp: RegExp;
  private srcAccessor: AbstractAccessor;

  constructor(
    public src: FileSystemAsync,
    public dst: FileSystemAsync,
    private options: SyncOptions = {}
  ) {
    if (options.excludeFileNamePattern == null)
      options.excludeFileNamePattern = "^\\..+$";
    if (options.verbose == null) options.verbose = false;
    this.excludeFileNameRegExp = new RegExp(options.excludeFileNamePattern);

    const srcFS = src.filesystem;
    this.srcAccessor = srcFS.accessor;
    if (!this.srcAccessor || !this.srcAccessor.options.index) {
      throw new Error(
        `Source filesystem "${srcFS.name}" has no index "${INDEX_FILE_PATH}"`
      );
    }

    const dstFS = dst.filesystem;
    this.dstAccessor = dstFS.accessor;
    if (!this.dstAccessor || !this.dstAccessor.options.index) {
      throw new Error(
        `Destination filesystem "${dstFS.name}" has no index "${INDEX_FILE_PATH}"`
      );
    }
  }

  async synchronizeAll() {
    await this.synchronizeDirectory(this.src.root.fullPath, true);
  }

  async synchronizeDirectory(dirPath: string, recursively: boolean) {
    if (!dirPath) {
      dirPath = DIR_SEPARATOR;
    }

    if (this.srcAccessor.options.verbose || this.dstAccessor.options.verbose) {
      console.log(`synchronize ${dirPath}`);
    }

    this.srcAccessor.clearContentsCache(dirPath);
    if (this.srcAccessor.options.shared) {
      await this.srcAccessor.loadDirPathIndex();
    } else {
      await this.srcAccessor.saveDirPathIndex();
    }
    const srcDirPathIndex = await this.srcAccessor.getDirPathIndex();

    this.dstAccessor.clearContentsCache(dirPath);
    if (this.dstAccessor.options.shared) {
      await this.dstAccessor.loadDirPathIndex();
    } else {
      await this.dstAccessor.saveDirPathIndex();
    }
    const dstDirPathIndex = await this.dstAccessor.getDirPathIndex();

    let [srcFileNameIndex, name] = await this.getDirPathIndex(
      this.srcAccessor,
      dirPath
    );
    let [dstFileNameIndex] = await this.getDirPathIndex(
      this.dstAccessor,
      dirPath
    );

    if (dirPath === DIR_SEPARATOR) {
      await this.synchronizeChildren(
        this.srcAccessor,
        srcDirPathIndex,
        this.dstAccessor,
        dstDirPathIndex,
        dirPath,
        recursively ? Number.MAX_VALUE : 0
      );
    } else {
      await this.synchronizeOne(
        this.srcAccessor,
        srcDirPathIndex,
        srcFileNameIndex,
        this.dstAccessor,
        dstDirPathIndex,
        dstFileNameIndex,
        name,
        recursively ? Number.MAX_VALUE : 1
      );
    }

    await this.srcAccessor.saveDirPathIndex();
    await this.dstAccessor.saveDirPathIndex();
  }

  private async copyFile(
    fromAccessor: AbstractAccessor,
    toAccessor: AbstractAccessor,
    fromRecord: Record
  ) {
    const obj = fromRecord.obj;
    this.debug(fromAccessor, toAccessor, "copyFile", obj.fullPath);
    const onCopy = this.options.onCopy;
    if (onCopy) {
      onCopy(fromAccessor.name, toAccessor.name, obj);
    }

    try {
      var content = await fromAccessor.doReadContent(obj.fullPath);
    } catch (e) {
      if (e instanceof NotFoundError) {
        console.warn(e, obj);
        await this.deleteEntry(fromAccessor, obj.fullPath, true);
        fromRecord.deleted = Date.now();
        await this.deleteEntry(toAccessor, obj.fullPath, true);
      } else {
        throw e;
      }
    }

    await toAccessor.doWriteContent(obj.fullPath, content);
  }

  private debug(
    fromAccessor: AbstractAccessor,
    toAccessor: AbstractAccessor,
    title: string,
    path: string
  ) {
    if (!this.options.verbose) {
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

  private deepCopy(obj: any) {
    return JSON.parse(JSON.stringify(obj));
  }

  private async deleteEntry(
    accessor: AbstractAccessor,
    fullPath: string,
    isFile: boolean
  ) {
    this.debug(null, accessor, "delete", fullPath);
    try {
      await accessor.doDelete(fullPath, isFile);
    } catch (e) {
      if (e instanceof NotFoundError) {
        console.info(e, fullPath);
      } else {
        throw e;
      }
    }
  }

  private async getDirPathIndex(
    accessor: AbstractAccessor,
    dirPath: string
  ): Promise<[FileNameIndex, string]> {
    const parentPath = getParentPath(dirPath);
    const name = getName(dirPath);
    try {
      var fileNameIndex = await accessor.getFileNameIndex(parentPath);
    } catch (e) {
      if (e instanceof NotFoundError) {
        fileNameIndex = {};
      } else {
        throw e;
      }
    }
    return [fileNameIndex, name];
  }

  private async synchronizeChildren(
    fromAccessor: AbstractAccessor,
    fromDirPathIndex: DirPathIndex,
    toAccessor: AbstractAccessor,
    toDirPathIndex: DirPathIndex,
    dirPath: string,
    recursiveCount: number
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

    const fromNames = Object.keys(fromFileNameIndex).filter(
      (name) => !this.excludeFileNameRegExp.test(name)
    );
    const toNames = Object.keys(toFileNameIndex).filter(
      (name) => !this.excludeFileNameRegExp.test(name)
    );
    outer: while (0 < fromNames.length) {
      const fromName = fromNames.shift();
      if (!fromName) {
        break;
      }

      // source to destination
      for (let i = 0, end = toNames.length; i < end; i++) {
        const toName = toNames[i];
        if (fromName !== toName) {
          continue;
        }

        await this.synchronizeOne(
          fromAccessor,
          fromDirPathIndex,
          fromFileNameIndex,
          toAccessor,
          toDirPathIndex,
          toFileNameIndex,
          fromName,
          recursiveCount
        );

        toNames.splice(i, 1);
        continue outer;
      }

      // destination not found.
      await this.synchronizeOne(
        fromAccessor,
        fromDirPathIndex,
        fromFileNameIndex,
        toAccessor,
        toDirPathIndex,
        toFileNameIndex,
        fromName,
        recursiveCount
      );
    }

    // source not found
    for (const toName of toNames) {
      await this.synchronizeOne(
        toAccessor,
        toDirPathIndex,
        toFileNameIndex,
        fromAccessor,
        fromDirPathIndex,
        fromFileNameIndex,
        toName,
        recursiveCount
      );
    }
  }

  private async synchronizeOne(
    fromAccessor: AbstractAccessor,
    fromDirPathIndex: DirPathIndex,
    fromFileNameIndex: FileNameIndex,
    toAccessor: AbstractAccessor,
    toDirPathIndex: DirPathIndex,
    toFileNameIndex: FileNameIndex,
    name: string,
    recursiveCount: number
  ) {
    let fromRecord = fromFileNameIndex[name];
    let toRecord = toFileNameIndex[name];

    let fromObj: FileSystemObject;
    let toObj: FileSystemObject;
    if (fromRecord != null && toRecord == null) {
      fromObj = fromRecord.obj;
      toRecord = this.deepCopy(fromRecord);
      delete toRecord.deleted;
      toRecord.updated = Synchronizer.NOT_EXISTS;
      toObj = toRecord.obj;
    } else if (fromRecord == null && toRecord != null) {
      toObj = toRecord.obj;
      fromRecord = this.deepCopy(toRecord);
      delete fromRecord.deleted;
      fromRecord.updated = Synchronizer.NOT_EXISTS;
      fromObj = fromRecord.obj;
    } else {
      fromObj = fromRecord.obj;
      toObj = toRecord.obj;
    }
    const fullPath = fromObj.fullPath;
    const fromDeleted = fromRecord.deleted;
    const toDeleted = toRecord.deleted;
    const fromUpdated = fromRecord.updated;
    const toUpdated = toRecord.updated;

    if (fullPath === INDEX_FILE_PATH) {
      return;
    }

    if (fromObj.size == null && toObj.size != null) {
      // TODO
      throw new Error("source is directory and destination is file");
    } else if (fromObj.size != null && toObj.size == null) {
      // TODO
      throw new Error("source is file and destination is directory");
    }

    try {
      if (fromObj.size != null) {
        // file
        if (fromDeleted != null && toDeleted == null) {
          if (fromDeleted <= toUpdated) {
            this.debug(fromAccessor, toAccessor, "file[1]", fullPath);
            await this.copyFile(toAccessor, fromAccessor, toRecord);
            fromFileNameIndex[name] = this.deepCopy(toRecord);
          } else {
            this.debug(fromAccessor, toAccessor, "file[2]", fullPath);
            if (toUpdated !== Synchronizer.NOT_EXISTS) {
              await this.deleteEntry(toAccessor, fullPath, true);
            }
            toFileNameIndex[name] = this.deepCopy(fromRecord);
          }
        } else if (fromDeleted == null && toDeleted != null) {
          if (toDeleted <= fromUpdated) {
            this.debug(fromAccessor, toAccessor, "file[3]", fullPath);
            await this.copyFile(fromAccessor, toAccessor, fromRecord);
            toFileNameIndex[name] = this.deepCopy(fromRecord);
          } else {
            this.debug(fromAccessor, toAccessor, "file[4]", fullPath);
            if (fromUpdated !== Synchronizer.NOT_EXISTS) {
              await this.deleteEntry(fromAccessor, fullPath, true);
            }
            fromFileNameIndex[name] = this.deepCopy(toRecord);
          }
        } else if (fromDeleted != null && toDeleted != null) {
          // prioritize old
          if (fromDeleted < toDeleted) {
            this.debug(fromAccessor, toAccessor, "file[5]", fullPath);
            toFileNameIndex[name] = this.deepCopy(fromRecord);
          } else if (toDeleted < fromDeleted) {
            this.debug(fromAccessor, toAccessor, "file[6]", fullPath);
            fromFileNameIndex[name] = this.deepCopy(toRecord);
          } else {
            this.debug(fromAccessor, toAccessor, "file[7]", fullPath);
          }
        } else {
          if (toUpdated < fromUpdated) {
            this.debug(fromAccessor, toAccessor, "file[8]", fullPath);
            await this.copyFile(fromAccessor, toAccessor, fromRecord);
            toFileNameIndex[name] = this.deepCopy(fromRecord);
          } else if (fromUpdated < toUpdated) {
            this.debug(fromAccessor, toAccessor, "file[9]", fullPath);
            await this.copyFile(toAccessor, fromAccessor, toRecord);
            fromFileNameIndex[name] = this.deepCopy(toRecord);
          } else {
            this.debug(fromAccessor, toAccessor, "file[10]", fullPath);
          }
        }
      } else {
        // directory
        if (fromDeleted != null && toDeleted == null) {
          if (fromDeleted <= toUpdated) {
            this.debug(fromAccessor, toAccessor, "dir[1]", fullPath);
            this.debug(null, fromAccessor, "doMakeDirectory", fullPath);
            await fromAccessor.doMakeDirectory(toObj);
            await this.synchronizeChildren(
              toAccessor,
              toDirPathIndex,
              fromAccessor,
              fromDirPathIndex,
              fullPath,
              Number.MAX_VALUE
            );
            fromFileNameIndex[name] = this.deepCopy(toRecord);
          } else {
            this.debug(fromAccessor, toAccessor, "dir[2]", fullPath);
            if (toUpdated !== Synchronizer.NOT_EXISTS) {
              // force synchronize recursively if delete directory
              await this.synchronizeChildren(
                fromAccessor,
                fromDirPathIndex,
                toAccessor,
                toDirPathIndex,
                fullPath,
                Number.MAX_VALUE
              );
              await this.deleteEntry(toAccessor, fullPath, false);
            }
            toFileNameIndex[name] = this.deepCopy(fromRecord);
          }
        } else if (fromDeleted == null && toDeleted != null) {
          if (toDeleted <= fromUpdated) {
            this.debug(fromAccessor, toAccessor, "dir[3]", fullPath);
            this.debug(null, toAccessor, "doMakeDirectory", fullPath);
            await toAccessor.doMakeDirectory(fromObj);
            await this.synchronizeChildren(
              fromAccessor,
              fromDirPathIndex,
              toAccessor,
              toDirPathIndex,
              fullPath,
              Number.MAX_VALUE
            );
            toFileNameIndex[name] = this.deepCopy(fromRecord);
          } else {
            this.debug(fromAccessor, toAccessor, "dir[4]", fullPath);
            if (fromUpdated !== Synchronizer.NOT_EXISTS) {
              // force synchronize recursively if delete directory
              await this.synchronizeChildren(
                fromAccessor,
                fromDirPathIndex,
                toAccessor,
                toDirPathIndex,
                fullPath,
                Number.MAX_VALUE
              );
              await this.deleteEntry(fromAccessor, fullPath, false);
            }
            fromFileNameIndex[name] = this.deepCopy(toRecord);
          }
        } else if (fromDeleted != null && toDeleted != null) {
          // prioritize old
          if (fromDeleted < toDeleted) {
            this.debug(fromAccessor, toAccessor, "dir[5]", fullPath);
            toFileNameIndex[name] = this.deepCopy(fromRecord);
          } else if (toDeleted < fromDeleted) {
            this.debug(fromAccessor, toAccessor, "dir[6]", fullPath);
            fromFileNameIndex[name] = this.deepCopy(toRecord);
          } else {
            this.debug(fromAccessor, toAccessor, "dir[7]", fullPath);
          }
        } else {
          if (toUpdated < fromUpdated) {
            this.debug(fromAccessor, toAccessor, "dir[8]", fullPath);
            toFileNameIndex[name] = this.deepCopy(fromRecord);
          } else if (fromUpdated < toUpdated) {
            this.debug(fromAccessor, toAccessor, "dir[9]", fullPath);
            fromFileNameIndex[name] = this.deepCopy(toRecord);
          } else {
            this.debug(fromAccessor, toAccessor, "dir[10]", fullPath);
          }

          // Directory is not found
          if (!toUpdated) {
            await toAccessor.doMakeDirectory(fromObj);
          } else if (!fromUpdated) {
            await fromAccessor.doMakeDirectory(toObj);
          }

          if (0 < recursiveCount) {
            await this.synchronizeChildren(
              fromAccessor,
              fromDirPathIndex,
              toAccessor,
              toDirPathIndex,
              fullPath,
              recursiveCount - 1
            );
          }
        }
      }
    } catch (e) {
      this.warn(fromAccessor, toAccessor, fromObj.fullPath, e);
    }
  }

  private warn(
    fromAccessor: AbstractAccessor,
    toAccessor: AbstractAccessor,
    path: string,
    e: any
  ) {
    if (!this.options.verbose) {
      return;
    }
    console.warn(
      `${fromAccessor.name} => ${toAccessor.name}: ${path}\n` +
        JSON.stringify(e)
    );
  }
}
