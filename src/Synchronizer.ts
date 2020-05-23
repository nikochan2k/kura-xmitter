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

  async synchronizeDirectory(dirPath: string, recursive: boolean) {
    if (!dirPath) {
      dirPath = "/";
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

    await this.synchronizeSelf(
      dirPath,
      this.srcAccessor,
      srcDirPathIndex,
      this.dstAccessor,
      dstDirPathIndex
    );

    await this.synchronize(
      dirPath,
      recursive,
      this.srcAccessor,
      srcDirPathIndex,
      this.dstAccessor,
      dstDirPathIndex
    );

    await this.srcAccessor.saveDirPathIndex();
    await this.dstAccessor.saveDirPathIndex();
  }

  private async copyFile(
    fromAccessor: AbstractAccessor,
    toAccessor: AbstractAccessor,
    obj: FileSystemObject
  ) {
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

        const dirPath = getParentPath(obj.fullPath);
        const deleted = Date.now();

        await this.deleteEntry(fromAccessor, obj.fullPath, true);
        const [fromFileNameIndex] = await this.getDirPathIndex(
          fromAccessor,
          dirPath
        );
        const fromRecord = fromFileNameIndex[obj.name];
        if (fromRecord) {
          fromRecord.deleted = deleted;
        }

        await this.deleteEntry(toAccessor, obj.fullPath, true);
        const [toFileNameIndex] = await this.getDirPathIndex(
          toAccessor,
          dirPath
        );
        const toRecord = toFileNameIndex[obj.name];
        if (toRecord) {
          toRecord.deleted = deleted;
        }
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
          recursive,
          fromAccessor,
          fromDirPathIndex,
          fromFileNameIndex,
          toAccessor,
          toDirPathIndex,
          toFileNameIndex,
          fromName
        );

        toNames.splice(i, 1);
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
        fromName
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
    const fromFullPath = fromObj.fullPath;
    const toFullPath = toObj.fullPath;
    const fromDeleted = fromRecord.deleted;
    const toDeleted = toRecord.deleted;
    const fromUpdated = fromRecord.updated;
    const toUpdated = toRecord.updated;

    if (fromFullPath === INDEX_FILE_PATH) {
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
            await this.copyFile(toAccessor, fromAccessor, toObj);
            fromFileNameIndex[name] = this.deepCopy(toRecord);
          } else if (toUpdated !== Synchronizer.NOT_EXISTS) {
            await this.deleteEntry(toAccessor, toFullPath, true);
            toFileNameIndex[name] = this.deepCopy(fromRecord);
          }
        } else if (fromDeleted == null && toDeleted != null) {
          if (toDeleted <= fromUpdated) {
            await this.copyFile(fromAccessor, toAccessor, fromObj);
            toFileNameIndex[name] = this.deepCopy(fromRecord);
          } else if (fromUpdated !== Synchronizer.NOT_EXISTS) {
            await this.deleteEntry(fromAccessor, fromFullPath, true);
            fromFileNameIndex[name] = this.deepCopy(toRecord);
          }
        } else if (fromDeleted != null && toDeleted != null) {
          // prioritize old
          if (fromDeleted < toDeleted) {
            toFileNameIndex[name] = this.deepCopy(fromRecord);
          } else if (toDeleted < fromDeleted) {
            fromFileNameIndex[name] = this.deepCopy(toRecord);
          }
        } else {
          if (toUpdated < fromUpdated) {
            await this.copyFile(fromAccessor, toAccessor, fromObj);
            toFileNameIndex[name] = this.deepCopy(fromRecord);
          } else if (fromUpdated < toUpdated) {
            await this.copyFile(toAccessor, fromAccessor, toObj);
            fromFileNameIndex[name] = this.deepCopy(toRecord);
          } else {
            if (fromObj.size !== toObj.size) {
              await this.copyFile(fromAccessor, toAccessor, fromObj);
              toFileNameIndex[name] = this.deepCopy(fromRecord);
            }
          }
        }
      } else {
        // directory
        if (fromDeleted != null && toDeleted == null) {
          if (fromDeleted <= toUpdated) {
            this.debug(null, fromAccessor, "putObject", toFullPath);
            await fromAccessor.doMakeDirectory(toObj);
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
            fromFileNameIndex[name] = this.deepCopy(toRecord);
          } else if (toUpdated !== Synchronizer.NOT_EXISTS) {
            // force synchronize recursively if delete directory
            await this.synchronize(
              toFullPath,
              true,
              toAccessor,
              toDirPathIndex,
              fromAccessor,
              fromDirPathIndex
            );
            await this.deleteEntry(toAccessor, toFullPath, false);
            toFileNameIndex[name] = this.deepCopy(fromRecord);
          }
        } else if (fromDeleted == null && toDeleted != null) {
          if (toDeleted <= fromUpdated) {
            this.debug(null, toAccessor, "putObject", toFullPath);
            await toAccessor.doMakeDirectory(fromObj);
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
            toFileNameIndex[name] = this.deepCopy(fromRecord);
          } else if (fromUpdated !== Synchronizer.NOT_EXISTS) {
            // force synchronize recursively if delete directory
            await this.synchronize(
              toFullPath,
              true,
              fromAccessor,
              fromDirPathIndex,
              toAccessor,
              toDirPathIndex
            );
            await this.deleteEntry(fromAccessor, fromFullPath, false);
            fromFileNameIndex[name] = this.deepCopy(toRecord);
          }
        } else if (fromDeleted != null && toDeleted != null) {
          // prioritize old
          if (fromDeleted < toDeleted) {
            toFileNameIndex[name] = this.deepCopy(fromRecord);
          } else if (toDeleted < fromDeleted) {
            fromFileNameIndex[name] = this.deepCopy(toRecord);
          }
        } else {
          if (fromUpdated < toUpdated) {
            fromFileNameIndex[name] = this.deepCopy(toRecord);
          } else if (toUpdated < fromUpdated) {
            toFileNameIndex[name] = this.deepCopy(fromRecord);
          }

          // Directory is not found
          if (!toRecord.updated) {
            await toAccessor.doMakeDirectory(fromObj);
          } else if (!fromRecord.updated) {
            await fromAccessor.doMakeDirectory(toObj);
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
    } catch (e) {
      this.warn(fromAccessor, toAccessor, fromObj.fullPath, e);
    }
  }

  private async synchronizeSelf(
    dirPath: string,
    fromAccessor: AbstractAccessor,
    fromDirPathIndex: DirPathIndex,
    toAccessor: AbstractAccessor,
    toDirPathIndex: DirPathIndex
  ) {
    if (dirPath === DIR_SEPARATOR) {
      return;
    }

    const [fromFileNameIndex, name] = await this.getDirPathIndex(
      fromAccessor,
      dirPath
    );
    const [toFileNameIndex] = await this.getDirPathIndex(toAccessor, dirPath);

    await this.synchronizeOne(
      false,
      fromAccessor,
      fromDirPathIndex,
      fromFileNameIndex,
      toAccessor,
      toDirPathIndex,
      toFileNameIndex,
      name
    );
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
