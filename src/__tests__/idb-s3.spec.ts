import { Transferer } from "kura";
import { Synchronizer } from "../Synchronizer";
import * as idbFS from "./idbFS";
import * as s3FS from "./s3FS";
import { testAll } from "./syncronize";

testAll("idb => s3", async () => {
  const local = await idbFS.getFileSystem();
  const remote = await s3FS.getFileSystem();

  const synchronizer = new Synchronizer(local, remote, {
    verbose: false,
    transferer: new Transferer(),
  });
  return { local, remote, synchronizer };
});
