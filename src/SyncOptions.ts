import { Transferer } from "kura";

export interface SyncOptions {
  excludeNamePattern?: string;
  excludePathPattern?: string;
  transferer?: Transferer;
  verbose?: boolean;
}
