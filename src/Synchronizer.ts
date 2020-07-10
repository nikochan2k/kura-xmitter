import {
  AbstractAccessor,
  DIR_SEPARATOR,
  FileNameIndex,
  FileSystemAsync,
  FileSystemObject,
  getName,
  getParentPath,
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
      options.excludeFileNamePattern = "^\\..+$|^$"; // TODO configurable
    if (options.verbose == null) options.verbose = false;
    this.excludeFileNameRegExp = new RegExp(options.excludeFileNamePattern);

    const srcFS = src.filesystem;
    this.srcAccessor = srcFS.accessor;
    if (!this.srcAccessor || !this.srcAccessor.options.index) {
      throw new Error(`Source filesystem "${srcFS.name}" has no index`);
    }

    const dstFS = dst.filesystem;
    this.dstAccessor = dstFS.accessor;
    if (!this.dstAccessor || !this.dstAccessor.options.index) {
      throw new Error(`Destination filesystem "${dstFS.name}" has no index`);
    }
  }

  async synchronizeAll() {
    await this.synchronizeDirectory(this.src.root.fullPath, true);
  }

  async synchronizeDirectory(
    dirPath: string,
    recursively: boolean
  ): Promise<boolean> {
    if (!dirPath) {
      dirPath = DIR_SEPARATOR;
    }

    this.srcAccessor.clearContentsCache(dirPath);
    if (this.srcAccessor.options.shared) {
      await this.srcAccessor.clearFileNameIndexes(dirPath);
    } else {
      await this.srcAccessor.saveFileNameIndexes(dirPath);
    }

    this.dstAccessor.clearContentsCache(dirPath);
    if (this.dstAccessor.options.shared) {
      await this.dstAccessor.clearFileNameIndexes(dirPath);
    } else {
      await this.dstAccessor.saveFileNameIndexes(dirPath);
    }

    let updated: boolean;
    if (dirPath === DIR_SEPARATOR) {
      updated = await this.synchronizeChildren(
        this.srcAccessor,
        this.dstAccessor,
        dirPath,
        recursively ? Number.MAX_VALUE : 0
      );
    } else {
      const {
        fileNameIndex: srcFileNameIndex,
        parentPath,
        name,
      } = await this.getFileNameIndex(this.srcAccessor, dirPath);
      const { fileNameIndex: dstFileNameIndex } = await this.getFileNameIndex(
        this.dstAccessor,
        dirPath
      );

      updated = await this.synchronizeOne(
        this.srcAccessor,
        srcFileNameIndex,
        this.dstAccessor,
        dstFileNameIndex,
        name,
        recursively ? Number.MAX_VALUE : 1
      );

      if (updated) {
        await this.srcAccessor.saveFileNameIndex(
          parentPath,
          srcFileNameIndex,
          true
        );
        await this.dstAccessor.saveFileNameIndex(
          parentPath,
          dstFileNameIndex,
          true
        );
      }
    }

    return updated;
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
      await onCopy(fromAccessor.name, toAccessor.name, obj);
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

  private async getFileNameIndex(accessor: AbstractAccessor, dirPath: string) {
    const parentPath = getParentPath(dirPath);
    const name = getName(dirPath);
    try {
      var fileNameIndex = await accessor.getFileNameIndex(parentPath);
    } catch (e) {
      if (!(e instanceof NotFoundError)) {
        throw e;
      }
      fileNameIndex = {};
    }
    return { fileNameIndex, parentPath, name };
  }

  private async synchronizeChildren(
    fromAccessor: AbstractAccessor,
    toAccessor: AbstractAccessor,
    dirPath: string,
    recursiveCount: number
  ): Promise<boolean> {
    let fromFileNameIndex: FileNameIndex;
    try {
      fromFileNameIndex = await fromAccessor.getFileNameIndex(dirPath);
    } catch (e) {
      this.warn(fromAccessor, toAccessor, dirPath, e);
      return false;
    }

    let toFileNameIndex: FileNameIndex;
    try {
      toFileNameIndex = await toAccessor.getFileNameIndex(dirPath);
    } catch (e) {
      this.warn(fromAccessor, toAccessor, dirPath, e);
      return false;
    }

    const fromNames = Object.keys(fromFileNameIndex).filter(
      (name) => !this.excludeFileNameRegExp.test(name)
    );
    const toNames = Object.keys(toFileNameIndex).filter(
      (name) => !this.excludeFileNameRegExp.test(name)
    );

    let updated = false;

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

        const result = await this.synchronizeOne(
          fromAccessor,
          fromFileNameIndex,
          toAccessor,
          toFileNameIndex,
          fromName,
          recursiveCount
        );
        updated = updated || result;

        toNames.splice(i, 1);
        continue outer;
      }

      // destination not found.
      const result = await this.synchronizeOne(
        fromAccessor,
        fromFileNameIndex,
        toAccessor,
        toFileNameIndex,
        fromName,
        recursiveCount
      );
      updated = updated || result;
    }

    // source not found
    for (const toName of toNames) {
      const result = await this.synchronizeOne(
        toAccessor,
        toFileNameIndex,
        fromAccessor,
        fromFileNameIndex,
        toName,
        recursiveCount
      );
      updated = updated || result;
    }

    if (updated) {
      await fromAccessor.saveFileNameIndex(dirPath, fromFileNameIndex, true);
      await toAccessor.saveFileNameIndex(dirPath, toFileNameIndex, true);
    }

    return updated;
  }

  private async synchronizeOne(
    fromAccessor: AbstractAccessor,
    fromFileNameIndex: FileNameIndex,
    toAccessor: AbstractAccessor,
    toFileNameIndex: FileNameIndex,
    name: string,
    recursiveCount: number
  ): Promise<boolean> {
    let updated = true;

    try {
      let fromRecord = fromFileNameIndex[name];
      let toRecord = toFileNameIndex[name];
      if (fromRecord == null && toRecord == null) {
        this.warn(fromAccessor, toAccessor, name, new Error("No records"));
        return false;
      }

      let fromObj: FileSystemObject;
      let toObj: FileSystemObject;
      if (fromRecord != null && toRecord == null) {
        fromObj = fromRecord.obj;
        toRecord = this.deepCopy(fromRecord);
        delete toRecord.deleted;
        toRecord.modified = Synchronizer.NOT_EXISTS;
        toObj = toRecord.obj;
      } else if (fromRecord == null && toRecord != null) {
        toObj = toRecord.obj;
        fromRecord = this.deepCopy(toRecord);
        delete fromRecord.deleted;
        fromRecord.modified = Synchronizer.NOT_EXISTS;
        fromObj = fromRecord.obj;
      } else {
        fromObj = fromRecord.obj;
        toObj = toRecord.obj;
      }
      const fullPath = fromObj.fullPath;
      const fromDeleted = fromRecord.deleted;
      const toDeleted = toRecord.deleted;
      const fromModified = fromRecord.modified;
      const toModified = toRecord.modified;

      if (fromObj.size == null && toObj.size != null) {
        this.warn(
          fromAccessor,
          toAccessor,
          fullPath,
          new Error("source is directory and destination is file")
        );
        return false;
      } else if (fromObj.size != null && toObj.size == null) {
        this.warn(
          fromAccessor,
          toAccessor,
          fullPath,
          new Error("source is file and destination is directory")
        );
        return false;
      }

      if (fromObj.size != null) {
        // file
        if (fromDeleted != null && toDeleted == null) {
          if (fromDeleted <= toModified) {
            this.debug(fromAccessor, toAccessor, "file[1]", fullPath);
            await this.copyFile(toAccessor, fromAccessor, toRecord);
            fromFileNameIndex[name] = this.deepCopy(toRecord);
          } else {
            this.debug(fromAccessor, toAccessor, "file[2]", fullPath);
            if (toModified !== Synchronizer.NOT_EXISTS) {
              await this.deleteEntry(toAccessor, fullPath, true);
            }
            toFileNameIndex[name] = this.deepCopy(fromRecord);
          }
        } else if (fromDeleted == null && toDeleted != null) {
          if (toDeleted <= fromModified) {
            this.debug(fromAccessor, toAccessor, "file[3]", fullPath);
            await this.copyFile(fromAccessor, toAccessor, fromRecord);
            toFileNameIndex[name] = this.deepCopy(fromRecord);
          } else {
            this.debug(fromAccessor, toAccessor, "file[4]", fullPath);
            if (fromModified !== Synchronizer.NOT_EXISTS) {
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
            updated = false;
          }
        } else {
          if (toModified < fromModified) {
            this.debug(fromAccessor, toAccessor, "file[8]", fullPath);
            await this.copyFile(fromAccessor, toAccessor, fromRecord);
            toFileNameIndex[name] = this.deepCopy(fromRecord);
          } else if (fromModified < toModified) {
            this.debug(fromAccessor, toAccessor, "file[9]", fullPath);
            await this.copyFile(toAccessor, fromAccessor, toRecord);
            fromFileNameIndex[name] = this.deepCopy(toRecord);
          } else {
            this.debug(fromAccessor, toAccessor, "file[10]", fullPath);
            updated = false;
          }
        }
      } else {
        // directory
        if (fromDeleted != null && toDeleted == null) {
          if (fromDeleted <= toModified) {
            this.debug(fromAccessor, toAccessor, "dir[1]", fullPath);
            this.debug(null, fromAccessor, "doMakeDirectory", fullPath);
            await fromAccessor.doMakeDirectory(toObj);
            await this.synchronizeChildren(
              toAccessor,
              fromAccessor,
              fullPath,
              Number.MAX_VALUE
            );
            fromFileNameIndex[name] = this.deepCopy(toRecord);
          } else {
            this.debug(fromAccessor, toAccessor, "dir[2]", fullPath);
            if (toModified !== Synchronizer.NOT_EXISTS) {
              // force synchronize recursively if delete directory
              await this.synchronizeChildren(
                fromAccessor,
                toAccessor,
                fullPath,
                Number.MAX_VALUE
              );
              await this.deleteEntry(toAccessor, fullPath, false);
            }
            toFileNameIndex[name] = this.deepCopy(fromRecord);
          }
        } else if (fromDeleted == null && toDeleted != null) {
          if (toDeleted <= fromModified) {
            this.debug(fromAccessor, toAccessor, "dir[3]", fullPath);
            this.debug(null, toAccessor, "doMakeDirectory", fullPath);
            await toAccessor.doMakeDirectory(fromObj);
            await this.synchronizeChildren(
              fromAccessor,
              toAccessor,
              fullPath,
              Number.MAX_VALUE
            );
            toFileNameIndex[name] = this.deepCopy(fromRecord);
          } else {
            this.debug(fromAccessor, toAccessor, "dir[4]", fullPath);
            if (fromModified !== Synchronizer.NOT_EXISTS) {
              // force synchronize recursively if delete directory
              await this.synchronizeChildren(
                fromAccessor,
                toAccessor,
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
            updated = false;
          }
        } else {
          if (toModified < fromModified) {
            this.debug(fromAccessor, toAccessor, "dir[8]", fullPath);
            toFileNameIndex[name] = this.deepCopy(fromRecord);
          } else if (fromModified < toModified) {
            this.debug(fromAccessor, toAccessor, "dir[9]", fullPath);
            fromFileNameIndex[name] = this.deepCopy(toRecord);
          } else {
            this.debug(fromAccessor, toAccessor, "dir[10]", fullPath);
            updated = false;
          }

          // Directory is not found
          if (!toModified) {
            await toAccessor.doMakeDirectory(fromObj);
          } else if (!fromModified) {
            await fromAccessor.doMakeDirectory(toObj);
          }

          if (0 < recursiveCount) {
            const result = await this.synchronizeChildren(
              fromAccessor,
              toAccessor,
              fullPath,
              recursiveCount - 1
            );
            updated = updated || result;
          }
        }
      }
    } catch (e) {
      this.warn(fromAccessor, toAccessor, name, e);
      return false;
    }

    return updated;
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
