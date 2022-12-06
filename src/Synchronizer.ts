import {
  AbstractAccessor,
  deepCopy,
  DIR_SEPARATOR,
  FileNameIndex,
  FileNameIndexEntry,
  FileSystemAsync,
  FileSystemObject,
  NotFoundError,
  Transferer,
} from "kura";
import { SyncOptions } from "./SyncOptions";

export interface SyncResult {
  backward: boolean;
  errors: any[];
  forward: boolean;
}

export interface Handler {
  afterCopy: (
    fromAccessor: AbstractAccessor,
    toAccessor: AbstractAccessor,
    obj: FileSystemObject
  ) => Promise<void>;
  afterDelete: (
    accessor: AbstractAccessor,
    obj: FileSystemObject
  ) => Promise<void>;
  beforeCopy: (
    fromAccessor: AbstractAccessor,
    toAccessor: AbstractAccessor,
    obj: FileSystemObject
  ) => Promise<boolean>;
  beforeDelete: (
    accessor: AbstractAccessor,
    obj: FileSystemObject
  ) => Promise<boolean>;
  completed: (result: SyncResult, error?: any) => Promise<void>;
  getNames: (fileNameIndex: FileNameIndex) => string[];
}

const DEFAULT_HANDLER: Handler = {
  afterCopy: async () => {},
  afterDelete: async () => {},
  beforeCopy: async () => false,
  beforeDelete: async () => false,
  completed: async (result) => {},
  getNames: (fileNameIndex) => {
    return Object.values(fileNameIndex)
      .sort((a, b) => b.modified - a.modified)
      .map((record) => record.name);
  },
};

export class Notifier {
  private _processed = 0;
  private _total = 0;

  constructor(private _callback = (processed: number, total: number) => {}) {}

  public get processed() {
    return this._processed;
  }

  public get total() {
    return this._total;
  }

  public incrementProcessed(count = 1) {
    this._processed = this._processed + count;
    this._callback(this._processed, this._total);
  }

  public incrementTotal(count = 1) {
    this._total = this._total + count;
    this._callback(this._processed, this._total);
  }
}

const DEFAULT_NOTIFIER = new Notifier();

export const SYNC_RESULT_FALSES: SyncResult = {
  forward: false,
  backward: false,
  errors: [],
};

export class Synchronizer {
  private static NOT_EXISTS = 0;

  private excludeNameRegExp: RegExp;
  private excludePathRegExp: RegExp;
  private localAccessor: AbstractAccessor;
  private remoteAccessor: AbstractAccessor;
  private transferer: Transferer;

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

    if (options.transferer) {
      this.transferer = options.transferer;
    } else {
      this.transferer = new Transferer();
    }
  }

  public async synchronizeAll() {
    return await this.synchronizeDirectory(this.local.root.fullPath, true);
  }

  public async synchronizeDirectory(
    dirPath: string,
    recursively: boolean,
    notifier = DEFAULT_NOTIFIER,
    handler = DEFAULT_HANDLER
  ): Promise<SyncResult> {
    try {
      if (!dirPath) {
        dirPath = DIR_SEPARATOR;
      }

      const result = await this.synchronizeChildren(
        this.localAccessor,
        this.remoteAccessor,
        dirPath,
        recursively,
        notifier,
        handler
      );

      this.debug(
        this.localAccessor,
        this.remoteAccessor,
        `SyncResult: localToRemote=${result.forward}, remoteToLocal=${result.backward}`,
        dirPath
      );

      await handler.completed(result);
      return result;
    } catch (e) {
      console.warn("synchronizeDirectory", { dirPath, recursively }, e);
      const result: SyncResult = { ...SYNC_RESULT_FALSES, errors: [e] };
      await handler.completed(result, e);
      return result;
    }
  }

  private async copyFile(
    fromAccessor: AbstractAccessor,
    toAccessor: AbstractAccessor,
    fromRecord: FileNameIndexEntry,
    handler: Handler
  ) {
    const obj = this.toFileSystemObject(fromRecord);
    if (await handler.beforeCopy(fromAccessor, toAccessor, obj)) {
      return;
    }

    toAccessor.clearContentsCache(obj.fullPath);
    await this.transferer.transfer(fromAccessor, obj, toAccessor, obj);
    await toAccessor.saveRecord(obj.fullPath, fromRecord);

    await handler.afterCopy(fromAccessor, toAccessor, obj);
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

  private async deleteEntry(
    accessor: AbstractAccessor,
    record: FileNameIndexEntry,
    handler: Handler
  ) {
    const obj = this.toFileSystemObject(record);
    const fullPath = obj.fullPath;
    const isFile = obj.size != null;
    this.debug(null, accessor, "delete", fullPath);
    try {
      if (await handler.beforeDelete(accessor, obj)) {
        return;
      }

      if (isFile) {
        await accessor.delete(fullPath, true);
      } else {
        await accessor.deleteRecursively(fullPath);
      }

      await handler.afterDelete(accessor, obj);
    } catch (e) {
      if (e instanceof NotFoundError) {
        console.info(e, fullPath);
      } else {
        throw e;
      }
    }
  }

  private async makeDirectory(
    accessor: AbstractAccessor,
    record: FileNameIndexEntry
  ) {
    const obj = this.toFileSystemObject(record);
    await accessor.doPutObject(obj);
    await accessor.saveRecord(obj.fullPath, { modified: record.modified });
  }

  private mergeResult(result: SyncResult, merged: SyncResult) {
    merged.forward = merged.forward || result.forward;
    merged.backward = merged.backward || result.backward;
    merged.errors = [...merged.errors, ...result.errors];
  }

  private async synchronizeChildren(
    fromAccessor: AbstractAccessor,
    toAccessor: AbstractAccessor,
    dirPath: string,
    recursively: boolean,
    notifier: Notifier,
    handler: Handler
  ): Promise<SyncResult> {
    if (this.excludePathRegExp.test(dirPath)) {
      return SYNC_RESULT_FALSES;
    }

    const fromFileNameIndex = await fromAccessor.getFileNameIndex(dirPath);
    const toFileNameIndex = await toAccessor.getFileNameIndex(dirPath);

    console.warn(fromFileNameIndex, toFileNameIndex);

    const fromNames = handler.getNames(fromFileNameIndex);
    notifier.incrementTotal(fromNames.length);
    const toNames = handler.getNames(toFileNameIndex);

    const fromToResult = deepCopy(SYNC_RESULT_FALSES);
    const toFromResult = deepCopy(SYNC_RESULT_FALSES);

    outer: for (const fromName of fromNames) {
      if (this.excludeNameRegExp.test(fromName)) {
        notifier.incrementProcessed();
        continue;
      }

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
          notifier,
          handler
        );
        this.mergeResult(oneResult, fromToResult);
        notifier.incrementProcessed();

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
        recursively,
        notifier,
        handler
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

      let oneResult = await this.synchronizeOne(
        toAccessor,
        toFileNameIndex,
        fromAccessor,
        fromFileNameIndex,
        toName,
        recursively,
        notifier,
        handler
      );
      this.mergeResult(oneResult, toFromResult);
      notifier.incrementProcessed();
    }

    const result: SyncResult = {
      forward: fromToResult.forward || toFromResult.backward,
      backward: fromToResult.backward || toFromResult.forward,
      errors: [...fromToResult.errors, ...toFromResult.errors],
    };

    return result;
  }

  private async synchronizeOne(
    fromAccessor: AbstractAccessor,
    fromFileNameIndex: FileNameIndex,
    toAccessor: AbstractAccessor,
    toFileNameIndex: FileNameIndex,
    name: string,
    recursively: boolean,
    notifier: Notifier,
    handler: Handler
  ): Promise<SyncResult> {
    const result = deepCopy(SYNC_RESULT_FALSES);

    try {
      let fromRecord = fromFileNameIndex[name];
      let toRecord = toFileNameIndex[name];
      if (fromRecord == null && toRecord == null) {
        this.warn(fromAccessor, toAccessor, name, "No records");
        return result;
      }

      if (fromRecord != null && toRecord == null) {
        toRecord = deepCopy(fromRecord);
        delete toRecord.deleted;
        toRecord.modified = Synchronizer.NOT_EXISTS;
      } else if (fromRecord == null && toRecord != null) {
        fromRecord = deepCopy(toRecord);
        delete fromRecord.deleted;
        fromRecord.modified = Synchronizer.NOT_EXISTS;
      }

      const fullPath = fromRecord.fullPath;
      const fromModified = fromRecord.modified;
      const toModified = toRecord.modified;

      if (fromRecord.size == null && toRecord.size != null) {
        this.warn(
          fromAccessor,
          toAccessor,
          fullPath,
          "source is directory and destination is file"
        );
        return result;
      } else if (fromRecord.size != null && toRecord.size == null) {
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
        this.debug(
          fromAccessor,
          toAccessor,
          (fromRecord.size != null ? "file" : "dir") + "[-from,-to]",
          fullPath
        );
        return result;
      }

      if (fromRecord.size != null) {
        // file
        if (fromDeleted != null && toDeleted == null) {
          if (fromDeleted < toModified) {
            try {
              await this.copyFile(toAccessor, fromAccessor, toRecord, handler);
              result.backward = true;
              this.debug(
                fromAccessor,
                toAccessor,
                "file[-from,to (-from < to) => +from]",
                fullPath
              );
            } catch (e) {
              if (e instanceof NotFoundError) {
                await toAccessor.deleteRecord(fullPath);
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
              await this.deleteEntry(toAccessor, toRecord, handler);
              result.forward = true;
              this.debug(
                fromAccessor,
                toAccessor,
                "file[-from,to (to <= -from) => -to]",
                fullPath
              );
            } else {
              if (fromAccessor === this.localAccessor) {
                // delete local index
                const indexPath = await fromAccessor.createIndexPath(fullPath);
                await fromAccessor.doDelete(indexPath, true);
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
              await this.copyFile(
                fromAccessor,
                toAccessor,
                fromRecord,
                handler
              );
              result.forward = true;
              this.debug(
                fromAccessor,
                toAccessor,
                "file[from,-to (-to < from) => +to]",
                fullPath
              );
            } catch (e) {
              if (e instanceof NotFoundError) {
                await fromAccessor.deleteRecord(fullPath);
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
              await this.deleteEntry(fromAccessor, fromRecord, handler);
              result.backward = true;
              this.debug(
                toAccessor,
                fromAccessor,
                "file[from,-to (from < -to) => -from]",
                fullPath
              );
            } else {
              if (toAccessor === this.localAccessor) {
                // delete local index
                const indexPath = await toAccessor.createIndexPath(fullPath);
                await toAccessor.doDelete(indexPath, true);
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
              await this.copyFile(
                fromAccessor,
                toAccessor,
                fromRecord,
                handler
              );
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
                  await this.copyFile(
                    toAccessor,
                    fromAccessor,
                    toRecord,
                    handler
                  );
                  result.backward = true;
                  this.debug(
                    fromAccessor,
                    toAccessor,
                    "file[from,to (!-from) => +from]",
                    fullPath
                  );
                } catch (e2) {
                  if (e2 instanceof NotFoundError) {
                    await this.deleteEntry(fromAccessor, fromRecord, handler);
                    await this.deleteEntry(toAccessor, toRecord, handler);
                    result.forward = true;
                    result.backward = true;
                    this.debug(
                      fromAccessor,
                      toAccessor,
                      "file[from,to (!-from,!-to) => -from,-to]",
                      fullPath
                    );
                  } else {
                    throw e2;
                  }
                }
              } else {
                throw e;
              }
            }
          } else if (fromModified < toModified) {
            try {
              await this.copyFile(toAccessor, fromAccessor, toRecord, handler);
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
                  await this.copyFile(
                    fromAccessor,
                    toAccessor,
                    toRecord,
                    handler
                  );
                  result.forward = true;
                  this.debug(
                    fromAccessor,
                    toAccessor,
                    "file[from,to (!-to) => +to]",
                    fullPath
                  );
                } catch (e2) {
                  if (e2 instanceof NotFoundError) {
                    await this.deleteEntry(fromAccessor, fromRecord, handler);
                    await this.deleteEntry(toAccessor, toRecord, handler);
                    result.forward = true;
                    result.backward = true;
                    this.debug(
                      fromAccessor,
                      toAccessor,
                      "file[from,to (!-from,!-to) => -from,-to]",
                      fullPath
                    );
                  } else {
                    throw e2;
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
            await this.makeDirectory(fromAccessor, toRecord);
            await this.synchronizeChildren(
              toAccessor,
              fromAccessor,
              fullPath,
              true,
              notifier,
              handler
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
              await this.deleteEntry(toAccessor, toRecord, handler);
              result.forward = true;
              this.debug(
                fromAccessor,
                toAccessor,
                "dir[-from,to (to < -from) => -to]",
                fullPath
              );
            } else {
              if (fromAccessor === this.localAccessor) {
                // delete local index
                const indexPath = await fromAccessor.createIndexPath(fullPath);
                await fromAccessor.doDelete(indexPath, true);
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
            await this.makeDirectory(toAccessor, fromRecord);
            await this.synchronizeChildren(
              fromAccessor,
              toAccessor,
              fullPath,
              true,
              notifier,
              handler
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
              await this.deleteEntry(fromAccessor, fromRecord, handler);
              result.backward = true;
              this.debug(
                toAccessor,
                fromAccessor,
                "dir[from,-to (from < -to) => -from]",
                fullPath
              );
            } else {
              if (toAccessor === this.localAccessor) {
                // delete local index
                const indexPath = await toAccessor.createIndexPath(fullPath);
                await toAccessor.doDelete(indexPath, true);
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
            await this.makeDirectory(fromAccessor, toRecord);
            result.backward = true;
            this.debug(
              fromAccessor,
              toAccessor,
              "dir[?from,to => +from]",
              fullPath
            );
          } else if (toModified === Synchronizer.NOT_EXISTS) {
            await this.makeDirectory(toAccessor, fromRecord);
            result.forward = true;
            this.debug(
              fromAccessor,
              toAccessor,
              "dir[from,?to => +to]",
              fullPath
            );
          } else if (toModified < fromModified) {
            await fromAccessor.saveRecord(fullPath, toRecord);
            result.backward = true;
            this.debug(
              fromAccessor,
              toAccessor,
              "dir[from,to (to < from) => +from]",
              fullPath
            );
          } else if (fromModified < toModified) {
            await toAccessor.saveRecord(fullPath, fromRecord);
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
              notifier,
              handler
            );
          }
        }
      }
    } catch (e) {
      result.errors.push(e);
      this.warn(fromAccessor, toAccessor, name, e);
    }

    return result;
  }

  private toFileSystemObject(record: FileNameIndexEntry) {
    const obj: FileSystemObject = {
      fullPath: record.fullPath,
      name: record.name,
      lastModified: record.modified,
      size: record.size,
    };
    return obj;
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
    const message = e ? JSON.stringify(e) : "";
    console.warn(
      `${fromAccessor.name} => ${toAccessor.name}: ${path}\n` + message
    );
  }
}
