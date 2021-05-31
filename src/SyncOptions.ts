import { FileSystemObject, Transferer } from "kura";

export interface SyncOptions {
  excludeNamePattern?: string;
  excludePathPattern?: string;
  onCopy?: (from: string, to: string, obj: FileSystemObject) => Promise<void>;
  transferer?: Transferer;
  verbose?: boolean;
}
