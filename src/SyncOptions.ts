import { FileSystemObject } from "kura";

export interface SyncOptions {
  excludeNamePattern?: string;
  excludePathPattern?: string;
  onCopy?: (from: string, to: string, obj: FileSystemObject) => Promise<void>;
  verbose?: boolean;
}
