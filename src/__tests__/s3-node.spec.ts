import { Transferer } from "kura";
import { Synchronizer } from "../Synchronizer";
import * as nodeFS from "./nodeFS";
import * as s3FS from "./s3FS";
import { testAll } from "./syncronize";

testAll("node => s3", async () => {
  const local = await nodeFS.getFileSystem();
  const remote = await s3FS.getFileSystem();

  const synchronizer = new Synchronizer(local, remote, {
    verbose: false,
    transferer: new Transferer(),
  });
  return { local: remote, remote: local, synchronizer };
});
