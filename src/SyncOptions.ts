import { FileSystemObject } from "kura";

export interface SyncOptions {
  excludeFileNamePattern?: string;
  onCopy?: (from: string, to: string, obj: FileSystemObject) => void;
  verbose?: boolean;
}
