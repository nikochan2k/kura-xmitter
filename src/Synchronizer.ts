import {
  AbstractAccessor,
  DIR_SEPARATOR,
  FileNameIndex,
  FileSystemAsync,
  FileSystemObject,
  NotFoundError,
  objectToText,
  Record,
  textToArrayBuffer,
  textToObject,
  toText,
} from "kura";
import { SyncOptions } from "./SyncOptions";

interface SyncResult {
  forward: boolean;
  backward: boolean;
}

interface LastSync {
  local: number;
  remote: number;
}

const LAST_SYNC_FILE_NAME = "sync.json";

export const SYNC_RESULT_FALSES: SyncResult = {
  forward: false,
  backward: false,
};

const NO_SYNC_RESULT = {
  local: NaN,
  remote: NaN,
};

export class Synchronizer {
  private static NOT_EXISTS = 0;

  private excludeFileNameRegExp: RegExp;
  private localAccessor: AbstractAccessor;
  private remoteAccessor: AbstractAccessor;

  constructor(
    public local: FileSystemAsync,
    public remote: FileSystemAsync,
    private options: SyncOptions = {}
  ) {
    if (options.excludeFileNamePattern == null)
      options.excludeFileNamePattern = "^\\..+$|^$"; // TODO configurable
    if (options.verbose == null) options.verbose = false;
    this.excludeFileNameRegExp = new RegExp(options.excludeFileNamePattern);

    const localFS = local.filesystem;
    this.localAccessor = localFS.accessor;
    if (!this.localAccessor || !this.localAccessor.options.index) {
      throw new Error(`Source filesystem "${localFS.name}" has no index`);
    }

    const remoteFS = remote.filesystem;
    this.remoteAccessor = remoteFS.accessor;
    if (!this.remoteAccessor || !this.remoteAccessor.options.index) {
      throw new Error(`Destination filesystem "${remoteFS.name}" has no index`);
    }
  }

  private async getLastSynchronized(dirPath: string) {
    const syncPath =
      this.localAccessor.createIndexDir(dirPath) + LAST_SYNC_FILE_NAME;
    try {
      const content = await this.localAccessor.doReadContent(syncPath);
      const text = await toText(content);
      const obj = textToObject(text) as LastSync;
      if (obj.local == null || obj.remote == null) {
        return NO_SYNC_RESULT;
      }
      return obj;
    } catch (e) {
      return NO_SYNC_RESULT;
    }
  }

  private async putLastSynchronized(dirPath: string) {
    const localPath = this.localAccessor.createIndexPath(dirPath);
    try {
      var localObj = await this.localAccessor.doGetObject(localPath);
    } catch (e) {
      if (e instanceof NotFoundError) {
        await this.localAccessor.saveFileNameIndex(dirPath);
        localObj = await this.localAccessor.doGetObject(localPath);
      } else {
        throw e;
      }
    }
    const remotePath = this.remoteAccessor.createIndexPath(dirPath);
    try {
      var remoteObj = await this.remoteAccessor.doGetObject(remotePath);
    } catch (e) {
      if (e instanceof NotFoundError) {
        await this.remoteAccessor.saveFileNameIndex(dirPath);
        remoteObj = await this.remoteAccessor.doGetObject(remotePath);
      } else {
        throw e;
      }
    }
    const lastSync: LastSync = {
      local: localObj.lastModified,
      remote: remoteObj.lastModified,
    };
    const text = objectToText(lastSync);
    const buffer = textToArrayBuffer(text);
    const indexDir = this.localAccessor.createIndexDir(dirPath);
    const syncPath = indexDir + LAST_SYNC_FILE_NAME;
    await this.localAccessor.doWriteContent(syncPath, buffer);
  }

  private mergeResult(newResult: SyncResult, result: SyncResult) {
    result.forward = result.forward || newResult.forward;
    result.backward = result.backward || newResult.backward;
  }

  async synchronizeAll() {
    return await this.synchronizeDirectory(this.local.root.fullPath, true);
  }

  async synchronizeDirectory(
    dirPath: string,
    recursively: boolean,
    deleteFlag = false
  ): Promise<SyncResult> {
    if (!dirPath) {
      dirPath = DIR_SEPARATOR;
    }

    const result = await this.synchronizeChildren(
      this.localAccessor,
      this.remoteAccessor,
      dirPath,
      recursively ? Number.MAX_VALUE : 0,
      deleteFlag
    );

    this.debug(
      this.localAccessor,
      this.remoteAccessor,
      `SyncResult: localToRemote=${result.forward}, remoteToLocal=${result.backward}`,
      dirPath
    );

    return result;
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

    const content = await fromAccessor.readContentInternal(obj);
    await toAccessor.clearContentsCache(obj.fullPath);
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

  private async synchronizeChildren(
    fromAccessor: AbstractAccessor,
    toAccessor: AbstractAccessor,
    dirPath: string,
    recursiveCount: number,
    deleteFlag: boolean
  ): Promise<SyncResult> {
    /*
    const lastSync = await this.getLastSynchronized(dirPath);
    try {
      const localObj = await this.localAccessor.getFileNameIndexObject(dirPath);
      const remoteObj = await this.remoteAccessor.getFileNameIndexObject(
        dirPath
      );
      if (
        lastSync.local === localObj.lastModified &&
        lastSync.remote === remoteObj.lastModified
      ) {
        this.debug(fromAccessor, toAccessor, "Not modified", dirPath);
        return SYNC_RESULT_FALSES;
      }
    } catch (e) {
      if (!(e instanceof NotFoundError)) {
        throw e;
      }
    }
    */

    try {
      if (fromAccessor.options.shared) {
        fromAccessor.clearFileNameIndex(dirPath);
      }
      var fromFileNameIndex = await fromAccessor.getFileNameIndex(dirPath);
      if (toAccessor.options.shared) {
        toAccessor.clearFileNameIndex(dirPath);
      }
      var toFileNameIndex = await toAccessor.getFileNameIndex(dirPath);
    } catch (e) {
      this.warn(fromAccessor, toAccessor, dirPath, e);
      return SYNC_RESULT_FALSES;
    }

    const fromNames = Object.keys(fromFileNameIndex).filter(
      (name) => !this.excludeFileNameRegExp.test(name)
    );
    const toNames = Object.keys(toFileNameIndex).filter(
      (name) => !this.excludeFileNameRegExp.test(name)
    );

    const result: SyncResult = { forward: false, backward: false };

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

        const oneResult = await this.synchronizeOne(
          fromAccessor,
          fromFileNameIndex,
          toAccessor,
          toFileNameIndex,
          fromName,
          recursiveCount,
          deleteFlag
        );
        this.mergeResult(oneResult, result);

        toNames.splice(i, 1);
        continue outer;
      }

      // destination not found.
      const oneResult = await this.synchronizeOne(
        fromAccessor,
        fromFileNameIndex,
        toAccessor,
        toFileNameIndex,
        fromName,
        recursiveCount,
        deleteFlag
      );
      this.mergeResult(oneResult, result);
    }

    // source not found
    for (const toName of toNames) {
      const oneResult = await this.synchronizeOne(
        toAccessor,
        toFileNameIndex,
        fromAccessor,
        fromFileNameIndex,
        toName,
        recursiveCount,
        deleteFlag
      );
      this.mergeResult(oneResult, result);
    }

    if (result.backward) {
      fromAccessor.dirPathIndex[dirPath] = fromFileNameIndex;
      await fromAccessor.saveFileNameIndex(dirPath);
    }
    if (result.forward) {
      toAccessor.dirPathIndex[dirPath] = toFileNameIndex;
      await toAccessor.saveFileNameIndex(dirPath);
    }

    // await this.putLastSynchronized(dirPath);

    return result;
  }

  private async synchronizeOne(
    fromAccessor: AbstractAccessor,
    fromFileNameIndex: FileNameIndex,
    toAccessor: AbstractAccessor,
    toFileNameIndex: FileNameIndex,
    name: string,
    recursiveCount: number,
    deleteFlag: boolean
  ): Promise<SyncResult> {
    const result: SyncResult = { forward: false, backward: false };

    try {
      let fromRecord = fromFileNameIndex[name];
      let toRecord = toFileNameIndex[name];
      if (fromRecord == null && toRecord == null) {
        this.warn(fromAccessor, toAccessor, name, new Error("No records"));
        return result;
      }

      let fromObj: FileSystemObject;
      let toObj: FileSystemObject;
      if (fromRecord != null && toRecord == null) {
        fromObj = fromRecord.obj;
        toRecord = this.deepCopy(fromRecord);
        if (deleteFlag) {
          toRecord.deleted = Date.now();
        } else {
          delete toRecord.deleted;
        }
        toRecord.modified = Synchronizer.NOT_EXISTS;
        toObj = toRecord.obj;
      } else if (fromRecord == null && toRecord != null) {
        toObj = toRecord.obj;
        fromRecord = this.deepCopy(toRecord);
        if (deleteFlag) {
          fromRecord.deleted = Date.now();
        } else {
          delete fromRecord.deleted;
        }
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
        return result;
      } else if (fromObj.size != null && toObj.size == null) {
        this.warn(
          fromAccessor,
          toAccessor,
          fullPath,
          new Error("source is file and destination is directory")
        );
        return result;
      }

      if (fromObj.size != null) {
        // file
        if (fromDeleted != null && toDeleted == null) {
          if (fromDeleted <= toModified) {
            this.debug(fromAccessor, toAccessor, "file[1]", fullPath);
            try {
              await this.copyFile(toAccessor, fromAccessor, toRecord);
              fromFileNameIndex[name] = this.deepCopy(toRecord);
              result.backward = true;
            } catch (e) {
              if (e instanceof NotFoundError) {
                if (toAccessor === this.remoteAccessor) {
                  toFileNameIndex[name] = { ...toRecord, deleted: Date.now() };
                  result.forward = true;
                } else {
                  delete toFileNameIndex[name];
                  result.backward = true;
                }
              } else {
                throw e;
              }
            }
          } else {
            this.debug(fromAccessor, toAccessor, "file[2]", fullPath);
            if (toModified !== Synchronizer.NOT_EXISTS) {
              await this.deleteEntry(toAccessor, fullPath, true);
            }
            if (toAccessor === this.remoteAccessor) {
              toFileNameIndex[name] = this.deepCopy(fromRecord);
              result.forward = true;
              delete fromFileNameIndex[name];
              result.backward = true;
            }
          }
        } else if (fromDeleted == null && toDeleted != null) {
          this.debug(fromAccessor, toAccessor, "file[4]", fullPath);
          if (fromModified !== Synchronizer.NOT_EXISTS) {
            await this.deleteEntry(fromAccessor, fullPath, true);
          }
          if (fromAccessor === this.remoteAccessor) {
            fromFileNameIndex[name] = this.deepCopy(toRecord);
            result.forward = true;
            delete toFileNameIndex[name];
            result.backward = true;
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
          if (toAccessor === this.remoteAccessor) {
            delete fromFileNameIndex[name];
            result.backward = true;
          } else {
            delete toFileNameIndex[name];
            result.backward = true;
          }
        } else {
          // fromDeleted == null && toDeleted = null
          if (toModified < fromModified) {
            this.debug(fromAccessor, toAccessor, "file[8]", fullPath);
            try {
              await this.copyFile(fromAccessor, toAccessor, fromRecord);
              toFileNameIndex[name] = this.deepCopy(fromRecord);
              result.forward = true;
            } catch (e) {
              if (e instanceof NotFoundError) {
                if (toAccessor === this.remoteAccessor) {
                  toFileNameIndex[name] = { ...toRecord, deleted: Date.now() };
                  result.forward = true;
                } else {
                  delete toFileNameIndex[name];
                  result.backward = true;
                }
              } else {
                throw e;
              }
            }
          } else if (fromModified < toModified) {
            this.debug(fromAccessor, toAccessor, "file[9]", fullPath);
            try {
              await this.copyFile(toAccessor, fromAccessor, toRecord);
              fromFileNameIndex[name] = this.deepCopy(toRecord);
              result.backward = true;
            } catch (e) {
              if (e instanceof NotFoundError) {
                if (fromAccessor === this.remoteAccessor) {
                  fromFileNameIndex[name] = {
                    ...fromRecord,
                    deleted: Date.now(),
                  };
                  result.forward = true;
                } else {
                  delete fromFileNameIndex[name];
                  result.backward = true;
                }
              } else {
                throw e;
              }
            }
          } else {
            this.debug(fromAccessor, toAccessor, "file[10]", fullPath);
          }
        }
      } else {
        // directory
        if (fromDeleted != null && toDeleted == null) {
          this.debug(fromAccessor, toAccessor, "dir[2]", fullPath);
          if (toModified !== Synchronizer.NOT_EXISTS) {
            // force synchronize recursively if delete directory
            await this.synchronizeChildren(
              fromAccessor,
              toAccessor,
              fullPath,
              Number.MAX_VALUE,
              true
            );
            await this.deleteEntry(toAccessor, fullPath, false);
          }
          if (toAccessor === this.remoteAccessor) {
            toFileNameIndex[name] = this.deepCopy(fromRecord);
            result.forward = true;
            delete fromFileNameIndex[name];
            result.backward = true;
          }
        } else if (fromDeleted == null && toDeleted != null) {
          this.debug(fromAccessor, toAccessor, "dir[4]", fullPath);
          if (fromModified !== Synchronizer.NOT_EXISTS) {
            // force synchronize recursively if delete directory
            await this.synchronizeChildren(
              fromAccessor,
              toAccessor,
              fullPath,
              Number.MAX_VALUE,
              true
            );
            await this.deleteEntry(fromAccessor, fullPath, false);
          }
          if (fromAccessor === this.remoteAccessor) {
            fromFileNameIndex[name] = this.deepCopy(toRecord);
            result.forward = true;
            delete toFileNameIndex[name];
            result.backward = true;
          }
        } else if (fromDeleted != null && toDeleted != null) {
          // prioritize old
          if (fromDeleted < toDeleted) {
            this.debug(fromAccessor, toAccessor, "dir[5]", fullPath);
            toFileNameIndex[name] = this.deepCopy(fromRecord);
            result.forward = true;
          } else if (toDeleted < fromDeleted) {
            this.debug(fromAccessor, toAccessor, "dir[6]", fullPath);
            fromFileNameIndex[name] = this.deepCopy(toRecord);
            result.backward = true;
          } else {
            this.debug(fromAccessor, toAccessor, "dir[7]", fullPath);
          }
          if (toAccessor === this.remoteAccessor) {
            delete fromFileNameIndex[name];
            result.backward = true;
          } else {
            delete toFileNameIndex[name];
            result.backward = true;
          }
        } else {
          if (toModified < fromModified) {
            this.debug(fromAccessor, toAccessor, "dir[8]", fullPath);
            toFileNameIndex[name] = this.deepCopy(fromRecord);
            result.forward = true;
          } else if (fromModified < toModified) {
            this.debug(fromAccessor, toAccessor, "dir[9]", fullPath);
            fromFileNameIndex[name] = this.deepCopy(toRecord);
            result.backward = true;
          } else {
            this.debug(fromAccessor, toAccessor, "dir[10]", fullPath);
          }

          // Directory is not found
          if (!toModified) {
            await toAccessor.doMakeDirectory(fromObj);
            result.forward = true;
          } else if (!fromModified) {
            await fromAccessor.doMakeDirectory(toObj);
            result.backward = true;
          }

          if (0 < recursiveCount) {
            await this.synchronizeChildren(
              fromAccessor,
              toAccessor,
              fullPath,
              recursiveCount - 1,
              false
            );
          }
        }
      }
    } catch (e) {
      this.warn(fromAccessor, toAccessor, name, e);
    }

    return result;
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
