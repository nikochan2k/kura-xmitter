import {
  AbstractAccessor,
  deepCopy,
  DIR_SEPARATOR,
  FileNameIndex,
  FileSystemAsync,
  FileSystemObject,
  getName,
  NotFoundError,
  Record,
} from "kura";
import { SyncOptions } from "./SyncOptions";

interface SyncResult {
  // #region Properties (2)

  localToRemote: boolean;
  remoteToLocal: boolean;

  // #endregion Properties (2)
}

export class Notifier {
  // #region Properties (2)

  private _processed = 0;
  private _total = 0;

  // #endregion Properties (2)

  // #region Constructors (1)

  constructor(private _callback = (processed: number, total: number) => {}) {}

  // #endregion Constructors (1)

  // #region Public Accessors (2)

  public get processed() {
    return this._processed;
  }

  public get total() {
    return this._total;
  }

  // #endregion Public Accessors (2)

  // #region Public Methods (2)

  public incrementProcessed(count = 1) {
    this._processed = this._processed + count;
    this._callback(this._processed, this._total);
  }

  public incrementTotal(count = 1) {
    this._total = this._total + count;
    this._callback(this._processed, this._total);
  }

  // #endregion Public Methods (2)
}

export const SYNC_RESULT_FALSES: SyncResult = {
  localToRemote: false,
  remoteToLocal: false,
};

export class Synchronizer {
  // #region Properties (4)

  private static NOT_EXISTS = 0;

  private excludeFileNameRegExp: RegExp;
  private localAccessor: AbstractAccessor;
  private remoteAccessor: AbstractAccessor;

  // #endregion Properties (4)

  // #region Constructors (1)

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

  // #endregion Constructors (1)

  // #region Public Methods (2)

  public async synchronizeAll() {
    return await this.synchronizeDirectory(this.local.root.fullPath, true);
  }

  public async synchronizeDirectory(
    dirPath: string,
    recursively: boolean,
    notifier = new Notifier()
  ): Promise<SyncResult> {
    if (!dirPath) {
      dirPath = DIR_SEPARATOR;
    }

    const result = await this.synchronizeChildren(
      this.localAccessor,
      this.remoteAccessor,
      dirPath,
      recursively,
      notifier
    );

    this.debug(
      this.localAccessor,
      this.remoteAccessor,
      `SyncResult: localToRemote=${result.localToRemote}, remoteToLocal=${result.remoteToLocal}`,
      dirPath
    );

    return result;
  }

  // #endregion Public Methods (2)

  // #region Private Methods (8)

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
      console.debug(
        `${fromAccessor.name} => ${toAccessor.name} - ${title}: ${path}`
      );
    } else {
      console.debug(`${toAccessor.name} - ${title}: ${path}`);
    }
  }

  private async deleteEntry(accessor: AbstractAccessor, obj: FileSystemObject) {
    const fullPath = obj.fullPath;
    const isFile = obj.size != null;
    this.debug(null, accessor, "delete", fullPath);
    try {
      if (isFile) {
        await accessor.delete(fullPath, true);
      } else {
        const objToDelete: FileSystemObject = {
          fullPath,
          name: getName(fullPath),
        };
        if (accessor === this.remoteAccessor) {
          await accessor.removeRecursively(objToDelete);
        } else {
          await accessor.removeRecursively(objToDelete);
          const indexDir = accessor.createIndexDir(fullPath);
          await accessor.removeRecursively({
            fullPath: indexDir,
            name: getName(fullPath),
          });
        }
      }
    } catch (e) {
      if (e instanceof NotFoundError) {
        console.info(e, fullPath);
      } else {
        throw e;
      }
    }
  }

  private mergeResult(newResult: SyncResult, result: SyncResult) {
    result.localToRemote = result.localToRemote || newResult.localToRemote;
    result.remoteToLocal = result.remoteToLocal || newResult.remoteToLocal;
  }

  private setResult(
    fromAccessor: AbstractAccessor,
    localToRemote: boolean,
    result: SyncResult
  ) {
    let newResult: SyncResult;
    if (fromAccessor === this.localAccessor) {
      newResult = {
        localToRemote: localToRemote,
        remoteToLocal: !localToRemote,
      };
    } else {
      newResult = {
        localToRemote: !localToRemote,
        remoteToLocal: localToRemote,
      };
    }
    this.mergeResult(newResult, result);
  }

  private async synchronizeChildren(
    fromAccessor: AbstractAccessor,
    toAccessor: AbstractAccessor,
    dirPath: string,
    recursively: boolean,
    notifier: Notifier
  ): Promise<SyncResult> {
    if (fromAccessor === this.remoteAccessor) {
      fromAccessor.clearFileNameIndex(dirPath);
    }
    const fromFileNameIndex = await fromAccessor.getFileNameIndex(dirPath);
    if (toAccessor === this.remoteAccessor) {
      toAccessor.clearFileNameIndex(dirPath);
    }
    const toFileNameIndex = await toAccessor.getFileNameIndex(dirPath);

    const fromNames = Object.keys(fromFileNameIndex);
    notifier.incrementTotal(fromNames.length);
    const toNames = Object.keys(toFileNameIndex);

    const result: SyncResult = { localToRemote: false, remoteToLocal: false };

    outer: for (const fromName of fromNames) {
      if (this.excludeFileNameRegExp.test(fromName)) {
        notifier.incrementProcessed();
        continue;
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
          recursively,
          notifier
        );
        this.mergeResult(oneResult, result);

        toNames.splice(i, 1);
        notifier.incrementProcessed();
        continue outer;
      }

      // destination not found.
      const oneResult = await this.synchronizeOne(
        fromAccessor,
        fromFileNameIndex,
        toAccessor,
        toFileNameIndex,
        fromName,
        recursively,
        notifier
      );
      this.mergeResult(oneResult, result);
      notifier.incrementProcessed();
    }

    // source not found
    notifier.incrementTotal(toNames.length);
    for (const toName of toNames) {
      if (this.excludeFileNameRegExp.test(toName)) {
        notifier.incrementProcessed();
        continue;
      }

      const oneResult = await this.synchronizeOne(
        toAccessor,
        toFileNameIndex,
        fromAccessor,
        fromFileNameIndex,
        toName,
        recursively,
        notifier
      );
      this.mergeResult(oneResult, result);
      notifier.incrementProcessed();
    }

    if (result.localToRemote) {
      if (toAccessor === this.remoteAccessor) {
        toAccessor.dirPathIndex[dirPath] = toFileNameIndex;
        await toAccessor.saveFileNameIndex(dirPath);
      }
      if (fromAccessor === this.remoteAccessor) {
        fromAccessor.dirPathIndex[dirPath] = fromFileNameIndex;
        await fromAccessor.saveFileNameIndex(dirPath);
      }
    }
    if (result.remoteToLocal) {
      if (fromAccessor === this.localAccessor) {
        fromAccessor.dirPathIndex[dirPath] = fromFileNameIndex;
        await fromAccessor.saveFileNameIndex(dirPath);
      }
      if (toAccessor === this.localAccessor) {
        toAccessor.dirPathIndex[dirPath] = toFileNameIndex;
        await toAccessor.saveFileNameIndex(dirPath);
      }
    }

    return result;
  }

  private async synchronizeOne(
    fromAccessor: AbstractAccessor,
    fromFileNameIndex: FileNameIndex,
    toAccessor: AbstractAccessor,
    toFileNameIndex: FileNameIndex,
    name: string,
    recursively: boolean,
    notifier: Notifier
  ): Promise<SyncResult> {
    const result: SyncResult = { localToRemote: false, remoteToLocal: false };

    try {
      let fromRecord = fromFileNameIndex[name];
      let toRecord = toFileNameIndex[name];
      if (fromRecord == null && toRecord == null) {
        this.warn(fromAccessor, toAccessor, name, "No records");
        return result;
      }

      let fromObj: FileSystemObject;
      let toObj: FileSystemObject;
      if (fromRecord != null && toRecord == null) {
        fromObj = fromRecord.obj;
        toRecord = deepCopy(fromRecord);
        delete toRecord.deleted;
        toRecord.modified = Synchronizer.NOT_EXISTS;
        toObj = toRecord.obj;
      } else if (fromRecord == null && toRecord != null) {
        toObj = toRecord.obj;
        fromRecord = deepCopy(toRecord);
        delete fromRecord.deleted;
        fromRecord.modified = Synchronizer.NOT_EXISTS;
        fromObj = fromRecord.obj;
      } else {
        fromObj = fromRecord.obj;
        toObj = toRecord.obj;
      }

      if (fromObj != null && toObj == null) {
        this.warn(fromAccessor, toAccessor, name, "No toObj");
        toObj = deepCopy(fromObj);
        toRecord.obj = toObj;
      } else if (toObj != null && fromObj == null) {
        this.warn(fromAccessor, toAccessor, name, "No fromObj");
        fromObj = deepCopy(toObj);
        fromRecord.obj = fromObj;
      } else if (fromObj == null && toObj == null) {
        this.warn(fromAccessor, toAccessor, name, "No obj");
        return result;
      }

      const fullPath = fromObj.fullPath;
      const fromDeleted = fromRecord.deleted;
      const toDeleted = toRecord.deleted;

      if (fromDeleted != null && toDeleted != null) {
        // prioritize old
        if (fromDeleted < toDeleted) {
          this.debug(fromAccessor, toAccessor, "delete[from < to]", fullPath);
          toFileNameIndex[name] = deepCopy(fromRecord);
        } else if (toDeleted < fromDeleted) {
          this.debug(fromAccessor, toAccessor, "delete[to < from]", fullPath);
          fromFileNameIndex[name] = deepCopy(toRecord);
        } else {
          this.debug(fromAccessor, toAccessor, "delete[from == to]", fullPath);
        }
        if (toAccessor === this.remoteAccessor) {
          delete fromFileNameIndex[name];
          result.remoteToLocal = true;
        } else {
          delete toFileNameIndex[name];
          result.remoteToLocal = true;
        }
        return result;
      }

      const fromModified = fromRecord.modified;
      const toModified = toRecord.modified;

      if (fromObj.size == null && toObj.size != null) {
        this.warn(
          fromAccessor,
          toAccessor,
          fullPath,
          "source is directory and destination is file"
        );
        return result;
      } else if (fromObj.size != null && toObj.size == null) {
        this.warn(
          fromAccessor,
          toAccessor,
          fullPath,
          "source is file and destination is directory"
        );
        return result;
      }

      if (fromObj.size != null) {
        // file
        if (fromDeleted != null && toDeleted == null) {
          if (fromDeleted < toModified) {
            this.debug(
              fromAccessor,
              toAccessor,
              "file[toObj => ~fromObj]",
              fullPath
            );
            try {
              await this.copyFile(toAccessor, fromAccessor, toRecord);
              fromFileNameIndex[name] = deepCopy(toRecord);
              this.setResult(fromAccessor, false, result);
            } catch (e) {
              if (e instanceof NotFoundError) {
                if (toAccessor === this.remoteAccessor) {
                  toFileNameIndex[name] = { ...toRecord, deleted: Date.now() };
                  result.localToRemote = true;
                } else {
                  delete toFileNameIndex[name];
                  result.remoteToLocal = true;
                }
              } else {
                throw e;
              }
            }
          } else {
            this.debug(fromAccessor, toAccessor, "file[~toObj]", fullPath);
            if (toModified !== Synchronizer.NOT_EXISTS) {
              await this.deleteEntry(toAccessor, toObj);
            }
            if (toAccessor === this.remoteAccessor) {
              toFileNameIndex[name] = deepCopy(fromRecord);
              result.localToRemote = true;
              delete fromFileNameIndex[name];
            } else {
              delete toFileNameIndex[name];
            }
            result.remoteToLocal = true;
          }
        } else if (fromDeleted == null && toDeleted != null) {
          if (toDeleted < fromModified) {
            this.debug(
              fromAccessor,
              toAccessor,
              "file[fromObj => ~toObj]",
              fullPath
            );
            try {
              await this.copyFile(fromAccessor, toAccessor, fromRecord);
              toFileNameIndex[name] = deepCopy(fromRecord);
              this.setResult(fromAccessor, true, result);
            } catch (e) {
              if (e instanceof NotFoundError) {
                if (fromAccessor === this.remoteAccessor) {
                  fromFileNameIndex[name] = {
                    ...fromRecord,
                    deleted: Date.now(),
                  };
                  result.localToRemote = true;
                } else {
                  delete fromFileNameIndex[name];
                  result.remoteToLocal = true;
                }
              } else {
                throw e;
              }
            }
          } else {
            this.debug(fromAccessor, toAccessor, "file[~toObj]", fullPath);
            if (fromModified !== Synchronizer.NOT_EXISTS) {
              await this.deleteEntry(fromAccessor, fromObj);
            }
            if (fromAccessor === this.remoteAccessor) {
              fromFileNameIndex[name] = deepCopy(toRecord);
              result.localToRemote = true;
              delete toFileNameIndex[name];
            } else {
              delete fromFileNameIndex[name];
            }
            result.remoteToLocal = true;
          }
        } else if (fromDeleted == null && toDeleted == null) {
          if (toModified < fromModified) {
            this.debug(
              fromAccessor,
              toAccessor,
              "file[fromObj => toObj]",
              fullPath
            );
            try {
              await this.copyFile(fromAccessor, toAccessor, fromRecord);
              toFileNameIndex[name] = deepCopy(fromRecord);
              this.setResult(fromAccessor, true, result);
            } catch (e) {
              if (e instanceof NotFoundError) {
                if (toAccessor === this.remoteAccessor) {
                  toFileNameIndex[name] = { ...toRecord, deleted: Date.now() };
                  result.localToRemote = true;
                } else {
                  delete toFileNameIndex[name];
                  result.remoteToLocal = true;
                }
              } else {
                throw e;
              }
            }
          } else if (fromModified < toModified) {
            this.debug(
              fromAccessor,
              toAccessor,
              "file[toObj => fromObj]",
              fullPath
            );
            try {
              await this.copyFile(toAccessor, fromAccessor, toRecord);
              fromFileNameIndex[name] = deepCopy(toRecord);
              this.setResult(fromAccessor, false, result);
            } catch (e) {
              if (e instanceof NotFoundError) {
                if (fromAccessor === this.remoteAccessor) {
                  fromFileNameIndex[name] = {
                    ...fromRecord,
                    deleted: Date.now(),
                  };
                  result.localToRemote = true;
                } else {
                  delete fromFileNameIndex[name];
                  result.remoteToLocal = true;
                }
              } else {
                throw e;
              }
            }
          } else {
            this.debug(
              fromAccessor,
              toAccessor,
              "file[fromObj == toObj]",
              fullPath
            );
          }
        }
      } else {
        // directory
        if (fromDeleted != null && toDeleted == null) {
          if (fromDeleted < toModified) {
            this.debug(
              fromAccessor,
              toAccessor,
              "dir[toObj => ~fromObj]",
              fullPath
            );
            this.debug(null, fromAccessor, "doMakeDirectory", fullPath);
            await fromAccessor.doMakeDirectory(toObj);
            await this.synchronizeChildren(
              toAccessor,
              fromAccessor,
              fullPath,
              true,
              notifier
            );
            fromFileNameIndex[name] = deepCopy(toRecord);
            this.setResult(fromAccessor, false, result);
          } else {
            this.debug(fromAccessor, toAccessor, "dir[~toObj]", fullPath);
            if (toModified !== Synchronizer.NOT_EXISTS) {
              await this.deleteEntry(toAccessor, toObj);
            }
            if (toAccessor === this.remoteAccessor) {
              toFileNameIndex[name] = deepCopy(fromRecord);
              result.localToRemote = true;
              if (fromModified !== Synchronizer.NOT_EXISTS) {
                try {
                  const indexDir = fromAccessor.createIndexDir(fullPath);
                  await fromAccessor.removeRecursively({
                    fullPath: indexDir,
                    name: getName(indexDir),
                  });
                } catch (e) {
                  if (e instanceof NotFoundError) {
                    console.info(e, fullPath);
                  } else {
                    throw e;
                  }
                }
              }
              delete fromFileNameIndex[name];
            } else {
              delete toFileNameIndex[name];
            }
            result.remoteToLocal = true;
          }
        } else if (fromDeleted == null && toDeleted != null) {
          if (toDeleted < fromModified) {
            this.debug(
              fromAccessor,
              toAccessor,
              "dir[fromObj => ~toObj]",
              fullPath
            );
            this.debug(null, toAccessor, "doMakeDirectory", fullPath);
            await toAccessor.doMakeDirectory(fromObj);
            await this.synchronizeChildren(
              fromAccessor,
              toAccessor,
              fullPath,
              true,
              notifier
            );
            toFileNameIndex[name] = deepCopy(fromRecord);
            this.setResult(fromAccessor, true, result);
          } else {
            this.debug(fromAccessor, toAccessor, "dir[~fromObj]", fullPath);
            if (fromModified !== Synchronizer.NOT_EXISTS) {
              await this.deleteEntry(fromAccessor, fromObj);
            }
            if (fromAccessor === this.remoteAccessor) {
              fromFileNameIndex[name] = deepCopy(toRecord);
              result.localToRemote = true;
              if (toModified !== Synchronizer.NOT_EXISTS) {
                try {
                  const indexDir = toAccessor.createIndexDir(fullPath);
                  await toAccessor.removeRecursively({
                    fullPath: indexDir,
                    name: getName(indexDir),
                  });
                } catch (e) {
                  if (e instanceof NotFoundError) {
                    console.info(e, fullPath);
                  } else {
                    throw e;
                  }
                }
              }
              delete toFileNameIndex[name];
            } else {
              delete fromFileNameIndex[name];
            }
            result.remoteToLocal = true;
          }
        } else if (fromDeleted == null && toDeleted == null) {
          if (toModified < fromModified) {
            this.debug(
              fromAccessor,
              toAccessor,
              "dir[toObj => fromObj]",
              fullPath
            );
            toFileNameIndex[name] = deepCopy(fromRecord);
            this.setResult(fromAccessor, false, result);
          } else if (fromModified < toModified) {
            this.debug(
              fromAccessor,
              toAccessor,
              "dir[fromObj => toObj]",
              fullPath
            );
            fromFileNameIndex[name] = deepCopy(toRecord);
            this.setResult(fromAccessor, true, result);
          } else {
            this.debug(
              fromAccessor,
              toAccessor,
              "dir[fromObj == toObj]",
              fullPath
            );
          }

          // Directory is not found
          if (!toModified) {
            await toAccessor.doMakeDirectory(fromObj);
            this.setResult(fromAccessor, true, result);
          } else if (!fromModified) {
            await fromAccessor.doMakeDirectory(toObj);
            this.setResult(fromAccessor, false, result);
          }

          if (recursively) {
            await this.synchronizeChildren(
              fromAccessor,
              toAccessor,
              fullPath,
              recursively,
              notifier
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
    const message = e ? new String(e) : "";
    console.warn(
      `${fromAccessor.name} => ${toAccessor.name}: ${path}\n` + message
    );
  }

  // #endregion Private Methods (8)
}
