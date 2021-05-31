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

  backward: boolean;
  forward: boolean;

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
  forward: false,
  backward: false,
};

export class Synchronizer {
  // #region Properties (5)

  private static NOT_EXISTS = 0;

  private excludeNameRegExp: RegExp;
  private excludePathRegExp: RegExp;
  private localAccessor: AbstractAccessor;
  private remoteAccessor: AbstractAccessor;

  // #endregion Properties (5)

  // #region Constructors (1)

  constructor(
    public local: FileSystemAsync,
    public remote: FileSystemAsync,
    private options: SyncOptions = {}
  ) {
    if (options.excludeNamePattern == null) {
      options.excludeNamePattern = "^\\.|^$";
    }
    if (options.excludePathPattern == null) {
      options.excludePathPattern = "\\/\\.";
    }
    if (options.verbose == null) options.verbose = false;

    this.excludeNameRegExp = new RegExp(options.excludeNamePattern);
    this.excludePathRegExp = new RegExp(options.excludePathPattern);

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
      `SyncResult: localToRemote=${result.forward}, remoteToLocal=${result.backward}`,
      dirPath
    );

    return result;
  }

  // #endregion Public Methods (2)

  // #region Private Methods (7)

  private async copyFile(
    fromAccessor: AbstractAccessor,
    toAccessor: AbstractAccessor,
    fromRecord: Record
  ) {
    const obj = fromRecord.obj;
    const onCopy = this.options.onCopy;
    if (onCopy) {
      await onCopy(fromAccessor.name, toAccessor.name, obj);
    }

    const content = await fromAccessor.readContentInternal(obj);
    await toAccessor.clearContentsCache(obj.fullPath);
    await toAccessor.doWriteContent(obj.fullPath, content);
    this.debug(fromAccessor, toAccessor, "copyFile", obj.fullPath);
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
          const indexDir = await accessor.createIndexDir(fullPath);
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

  private mergeResult(result: SyncResult, merged: SyncResult) {
    merged.forward = merged.forward || result.forward;
    merged.backward = merged.backward || result.backward;
  }

  private async synchronizeChildren(
    fromAccessor: AbstractAccessor,
    toAccessor: AbstractAccessor,
    dirPath: string,
    recursively: boolean,
    notifier: Notifier
  ): Promise<SyncResult> {
    if (this.excludePathRegExp.test(dirPath)) {
      return { forward: false, backward: false };
    }

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

    const fromToResult: SyncResult = { forward: false, backward: false };
    const toFromResult: SyncResult = { forward: false, backward: false };
    outer: for (const fromName of fromNames) {
      if (this.excludeNameRegExp.test(fromName)) {
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
        this.mergeResult(oneResult, fromToResult);

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
      this.mergeResult(oneResult, fromToResult);
      notifier.incrementProcessed();
    }

    // source not found
    notifier.incrementTotal(toNames.length);
    for (const toName of toNames) {
      if (this.excludeNameRegExp.test(toName)) {
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
      this.mergeResult(oneResult, toFromResult);
      notifier.incrementProcessed();
    }

    const result: SyncResult = {
      forward: fromToResult.forward || toFromResult.backward,
      backward: fromToResult.backward || toFromResult.forward,
    };
    if (result.forward) {
      toAccessor.dirPathIndex[dirPath] = toFileNameIndex;
      await toAccessor.saveFileNameIndex(dirPath);
    }
    if (result.backward) {
      fromAccessor.dirPathIndex[dirPath] = fromFileNameIndex;
      await fromAccessor.saveFileNameIndex(dirPath);
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
    const result: SyncResult = { forward: false, backward: false };

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

      const fromDeleted = fromRecord.deleted;
      const toDeleted = toRecord.deleted;

      if (fromDeleted != null && toDeleted != null) {
        const prefix = fromObj.size != null ? "file[" : "dir[";
        // prioritize old
        if (fromDeleted < toDeleted) {
          if (toAccessor === this.remoteAccessor) {
            toFileNameIndex[name] = deepCopy(fromRecord);
            delete fromFileNameIndex[name];
            result.backward = true;
          } else {
            delete toFileNameIndex[name];
          }
          result.forward = true;
          this.debug(
            fromAccessor,
            toAccessor,
            prefix + "-from,-to (from < to) =>]",
            fullPath
          );
        } else if (toDeleted < fromDeleted) {
          fromFileNameIndex[name] = deepCopy(toRecord);
          if (fromAccessor === this.remoteAccessor) {
            fromFileNameIndex[name] = deepCopy(toRecord);
            delete toFileNameIndex[name];
            result.forward = true;
          } else {
            delete fromFileNameIndex[name];
          }
          result.backward = true;
          this.debug(
            fromAccessor,
            toAccessor,
            prefix + "-from,-to (to < from) =>]",
            fullPath
          );
        } else {
          this.debug(
            fromAccessor,
            toAccessor,
            prefix + "-from,-to (from == to) =>]",
            fullPath
          );
        }
        return result;
      }

      if (fromObj.size != null) {
        // file
        if (fromDeleted != null && toDeleted == null) {
          if (fromDeleted < toModified) {
            try {
              await this.copyFile(toAccessor, fromAccessor, toRecord);
              fromFileNameIndex[name] = deepCopy(toRecord);
              result.backward = true;
              this.debug(
                fromAccessor,
                toAccessor,
                "file[-from,to (-from < to) => +from]",
                fullPath
              );
            } catch (e) {
              if (e instanceof NotFoundError) {
                if (toAccessor === this.remoteAccessor) {
                  toFileNameIndex[name] = { ...toRecord, deleted: Date.now() };
                } else {
                  delete toFileNameIndex[name];
                }
                result.forward = true;
                this.debug(
                  fromAccessor,
                  toAccessor,
                  "file[-from,to (-to!) => -to]",
                  fullPath
                );
              } else {
                throw e;
              }
            }
          } else {
            if (toModified !== Synchronizer.NOT_EXISTS) {
              await this.deleteEntry(toAccessor, toObj);
              if (toAccessor === this.remoteAccessor) {
                toFileNameIndex[name] = deepCopy(fromRecord);
                delete fromFileNameIndex[name];
                result.backward = true;
              } else {
                delete toFileNameIndex[name];
              }
              result.forward = true;
              this.debug(
                fromAccessor,
                toAccessor,
                "file[-from,to (to <= -from) => -to]",
                fullPath
              );
            } else {
              if (fromAccessor === this.localAccessor) {
                delete fromFileNameIndex[name];
                result.backward = true;
              }
              this.debug(
                fromAccessor,
                toAccessor,
                "file[-from,?to =>]",
                fullPath
              );
            }
          }
        } else if (fromDeleted == null && toDeleted != null) {
          if (toDeleted < fromModified) {
            try {
              await this.copyFile(fromAccessor, toAccessor, fromRecord);
              toFileNameIndex[name] = deepCopy(fromRecord);
              result.forward = true;
              this.debug(
                fromAccessor,
                toAccessor,
                "file[from,-to (-to < from) => +to]",
                fullPath
              );
            } catch (e) {
              if (e instanceof NotFoundError) {
                if (fromAccessor === this.remoteAccessor) {
                  fromFileNameIndex[name] = {
                    ...fromRecord,
                    deleted: Date.now(),
                  };
                } else {
                  delete fromFileNameIndex[name];
                }
                result.backward = true;
                this.debug(
                  fromAccessor,
                  toAccessor,
                  "file[from,-to (-from!) => -from]",
                  fullPath
                );
              } else {
                throw e;
              }
            }
          } else {
            if (fromModified !== Synchronizer.NOT_EXISTS) {
              await this.deleteEntry(fromAccessor, fromObj);
              if (fromAccessor === this.remoteAccessor) {
                fromFileNameIndex[name] = deepCopy(toRecord);
                delete toFileNameIndex[name];
                result.forward = true;
              } else {
                delete fromFileNameIndex[name];
              }
              result.backward = true;
              this.debug(
                toAccessor,
                fromAccessor,
                "file[from,-to (from < -to) => -from]",
                fullPath
              );
            } else {
              if (toAccessor === this.localAccessor) {
                delete toFileNameIndex[name];
                result.backward = true;
              }
              this.debug(
                toAccessor,
                fromAccessor,
                "file[?from,-to =>]",
                fullPath
              );
            }
          }
        } else if (fromDeleted == null && toDeleted == null) {
          if (toModified < fromModified) {
            try {
              await this.copyFile(fromAccessor, toAccessor, fromRecord);
              toFileNameIndex[name] = deepCopy(fromRecord);
              result.forward = true;
              this.debug(
                fromAccessor,
                toAccessor,
                "file[from,to (to < from) => +to]",
                fullPath
              );
            } catch (e) {
              if (e instanceof NotFoundError) {
                try {
                  await this.copyFile(toAccessor, fromAccessor, toRecord);
                  fromFileNameIndex[name] = deepCopy(toRecord);
                  result.backward = true;
                  this.debug(
                    fromAccessor,
                    toAccessor,
                    "file[from,to (!-from) => +from]",
                    fullPath
                  );
                } catch (e2) {
                  if (e2 instanceof NotFoundError) {
                    await this.deleteEntry(fromAccessor, fromObj);
                    await this.deleteEntry(toAccessor, toObj);
                    const now = Date.now();
                    if (fromAccessor === this.localAccessor) {
                      delete fromFileNameIndex[name];
                      toRecord.deleted = now;
                    } else {
                      delete toFileNameIndex[name];
                      fromRecord.deleted = now;
                    }
                    result.forward = true;
                    result.backward = true;
                    this.debug(
                      fromAccessor,
                      toAccessor,
                      "file[from,to (!-from,!-to) => -from,-to]",
                      fullPath
                    );
                  }
                }
              } else {
                throw e;
              }
            }
          } else if (fromModified < toModified) {
            try {
              await this.copyFile(toAccessor, fromAccessor, toRecord);
              fromFileNameIndex[name] = deepCopy(toRecord);
              result.backward = true;
              this.debug(
                fromAccessor,
                toAccessor,
                "file[from,to (from < to) => +from]",
                fullPath
              );
            } catch (e) {
              if (e instanceof NotFoundError) {
                try {
                  await this.copyFile(fromAccessor, toAccessor, toRecord);
                  toFileNameIndex[name] = deepCopy(fromRecord);
                  result.forward = true;
                  this.debug(
                    fromAccessor,
                    toAccessor,
                    "file[from,to (!-to) => +to]",
                    fullPath
                  );
                } catch (e2) {
                  if (e2 instanceof NotFoundError) {
                    await this.deleteEntry(fromAccessor, fromObj);
                    await this.deleteEntry(toAccessor, toObj);
                    const now = Date.now();
                    if (fromAccessor === this.localAccessor) {
                      delete fromFileNameIndex[name];
                      toRecord.deleted = now;
                    } else {
                      delete toFileNameIndex[name];
                      fromRecord.deleted = now;
                    }
                    result.forward = true;
                    result.backward = true;
                    this.debug(
                      fromAccessor,
                      toAccessor,
                      "file[from,to (!-from,!-to) => -from,-to]",
                      fullPath
                    );
                  }
                }
              } else {
                throw e;
              }
            }
          } else {
            this.debug(
              fromAccessor,
              toAccessor,
              "file[from,to (from == to) =>]",
              fullPath
            );
          }
        }
      } else {
        // directory
        if (fromDeleted != null && toDeleted == null) {
          if (fromDeleted < toModified) {
            await fromAccessor.doMakeDirectory(toObj);
            fromFileNameIndex[name] = deepCopy(toRecord);
            await this.synchronizeChildren(
              toAccessor,
              fromAccessor,
              fullPath,
              true,
              notifier
            );
            result.backward = true;
            this.debug(
              fromAccessor,
              toAccessor,
              "dir[-from,to (-from < to) => +from]",
              fullPath
            );
          } else {
            if (toModified !== Synchronizer.NOT_EXISTS) {
              await this.deleteEntry(toAccessor, toObj);
              if (toAccessor === this.remoteAccessor) {
                toFileNameIndex[name] = deepCopy(fromRecord); // modify remote
                delete fromFileNameIndex[name]; // modify local
                result.backward = true;
              } else {
                delete toFileNameIndex[name]; // modify local
              }
              result.forward = true;
              this.debug(
                fromAccessor,
                toAccessor,
                "dir[-from,to (to < -from) => -to]",
                fullPath
              );
            } else {
              if (fromAccessor === this.localAccessor) {
                delete fromFileNameIndex[name];
                result.backward = true;
              }
              this.debug(
                fromAccessor,
                toAccessor,
                "dir[-from,?to =>]",
                fullPath
              );
            }
          }
        } else if (fromDeleted == null && toDeleted != null) {
          if (toDeleted < fromModified) {
            await toAccessor.doMakeDirectory(fromObj);
            toFileNameIndex[name] = deepCopy(fromRecord);
            await this.synchronizeChildren(
              fromAccessor,
              toAccessor,
              fullPath,
              true,
              notifier
            );
            result.forward = true;
            this.debug(
              fromAccessor,
              toAccessor,
              "dir[from,-to (-to < from) => +to]",
              fullPath
            );
          } else {
            if (fromModified !== Synchronizer.NOT_EXISTS) {
              await this.deleteEntry(fromAccessor, fromObj);
              if (fromAccessor === this.remoteAccessor) {
                fromFileNameIndex[name] = deepCopy(toRecord); // modify remote
                delete toFileNameIndex[name]; // modify local
                result.forward = true;
              } else {
                delete fromFileNameIndex[name]; // modify local
              }
              result.backward = true;
              this.debug(
                toAccessor,
                fromAccessor,
                "dir[from,-to (from < -to) => -from]",
                fullPath
              );
            } else {
              if (toAccessor === this.localAccessor) {
                delete toFileNameIndex[name];
                result.forward = true;
              }
              this.debug(
                toAccessor,
                fromAccessor,
                "dir[?from,-to =>]",
                fullPath
              );
            }
          }
        } else if (fromDeleted == null && toDeleted == null) {
          // prioritize older
          if (fromModified === Synchronizer.NOT_EXISTS) {
            await fromAccessor.doMakeDirectory(toObj);
            if (!toRecord.modified) {
              toRecord.modified = Date.now();
              result.forward = true;
            }
            fromFileNameIndex[name] = deepCopy(toRecord);
            result.backward = true;
            this.debug(
              fromAccessor,
              toAccessor,
              "dir[?from,to => +from]",
              fullPath
            );
          } else if (toModified === Synchronizer.NOT_EXISTS) {
            await toAccessor.doMakeDirectory(fromObj);
            if (!fromRecord.modified) {
              fromRecord.modified = Date.now();
              result.backward = true;
            }
            toFileNameIndex[name] = deepCopy(fromRecord);
            result.forward = true;
            this.debug(
              fromAccessor,
              toAccessor,
              "dir[from,?to => +to]",
              fullPath
            );
          } else if (toModified < fromModified) {
            fromFileNameIndex[name] = deepCopy(toRecord);
            result.backward = true;
            this.debug(
              fromAccessor,
              toAccessor,
              "dir[from,to (to < from) => +from]",
              fullPath
            );
          } else if (fromModified < toModified) {
            toFileNameIndex[name] = deepCopy(fromRecord);
            result.forward = true;
            this.debug(
              fromAccessor,
              toAccessor,
              "dir[from,to (from < to) => +to]",
              fullPath
            );
          } else {
            this.debug(
              fromAccessor,
              toAccessor,
              "dir[from,to (from == to) =>]",
              fullPath
            );
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

  // #endregion Private Methods (7)
}
