import { FileSystemAsync, NotFoundError } from "kura";
import { Synchronizer } from "../Synchronizer";

export function testAll(
  prepare: () => Promise<{ local: FileSystemAsync; remote: FileSystemAsync }>
) {
  let local: FileSystemAsync;
  let remote: FileSystemAsync;
  let synchronizer: Synchronizer;

  beforeAll(async () => {
    const result = await prepare();
    local = result.local;
    remote = result.remote;
    synchronizer = new Synchronizer(local, remote, { verbose: true });
  });

  test("add a empty file, sync all", async (done) => {
    try {
      let localFE = await local.root.getFile("empty.txt", {
        create: true,
        exclusive: true,
      });
      console.log((await local.root.createReader().readEntries()).length);
      await synchronizer.synchronizeAll();

      localFE = await local.root.getFile("empty.txt");
      const remoteReader = remote.root.createReader();
      const remoteEntries = await remoteReader.readEntries();
      expect(remoteEntries.length).toBe(1);
      const remoteE = remoteEntries[0];
      expect(remoteE.isFile).toBe(true);
      expect(remoteE.name).toBe(localFE.name);
      expect(remoteE.fullPath).toBe(localFE.fullPath);

      const localMeta = await localFE.getMetadata();
      const remoteMeta = await remoteE.getMetadata();
      expect(remoteMeta.size).toBe(localMeta.size);
    } catch (e) {
      fail(e);
    } finally {
      done();
    }
  });

  test("add a text file, sync all", async (done) => {
    try {
      let localFE = await local.root.getFile("test.txt", {
        create: true,
        exclusive: true,
      });
      const writer = await localFE.createWriter();
      await writer.writeFile(new Blob(["hoge"], { type: "text/plain" }));

      await synchronizer.synchronizeAll();

      const remoteReader = remote.root.createReader();
      const remoteEntries = await remoteReader.readEntries();
      expect(remoteEntries.length).toBe(2);

      let entries = remoteEntries.filter((re) => {
        return re.name === "empty.txt";
      });
      expect(entries.length).toBe(1);
      const emptyTxt = entries[0];
      expect(emptyTxt.isFile).toBe(true);
      const emptyTxtMeta = await emptyTxt.getMetadata();
      expect(emptyTxtMeta.size).toBe(0);

      entries = remoteEntries.filter((re) => {
        return re.name === "test.txt";
      });
      expect(entries.length).toBe(1);
      const testTxt = entries[0];
      expect(testTxt.isFile).toBe(true);
      localFE = await local.root.getFile("test.txt");
      const localFEMeta = await localFE.getMetadata();
      const testTxtMeta = await testTxt.getMetadata();
      expect(testTxtMeta.size).toBe(localFEMeta.size);
    } catch (e) {
      fail(e);
    } finally {
      done();
    }
  });

  test("add a hidden file, sync all", async (done) => {
    try {
      await local.root.getFile(".hidden", {
        create: true,
        exclusive: true,
      });

      await synchronizer.synchronizeAll();

      const remoteReader = remote.root.createReader();
      const remoteEntries = await remoteReader.readEntries();
      expect(remoteEntries.length).toBe(2);
    } catch (e) {
      fail(e);
    } finally {
      done();
    }
  });

  test("create folder, and add a text file, sync all", async (done) => {
    try {
      let localDE = await local.root.getDirectory("folder", {
        create: true,
        exclusive: true,
      });
      let localFE = await localDE.getFile("in.txt", {
        create: true,
        exclusive: true,
      });
      const writer = await localFE.createWriter();
      await writer.writeFile(new Blob(["hoge"], { type: "text/plain" }));

      await synchronizer.synchronizeAll();

      const localMeta = await localFE.getMetadata();
      const remoteDE = await remote.root.getDirectory("folder");
      const remoteFE = await remoteDE.getFile("in.txt");
      const remoteMeta = await remoteFE.getMetadata();
      expect(remoteMeta.size).toBe(localMeta.size);
    } catch (e) {
      fail(e);
    } finally {
      done();
    }
  });

  test("create nested folder, and add a empty file, sync dir", async (done) => {
    try {
      const localParentDE = await local.root.getDirectory("folder");
      const localFE = await localParentDE.getFile("fuga.txt", {
        create: true,
        exclusive: true,
      });
      let writer = await localFE.createWriter();
      await writer.writeFile(new Blob(["fuga"], { type: "text/plain" }));

      const localDE = await localParentDE.getDirectory("nested", {
        create: true,
        exclusive: true,
      });
      const nestedFE = await localDE.getFile("nested.txt", {
        create: true,
        exclusive: true,
      });
      writer = await nestedFE.createWriter();
      await writer.writeFile(new Blob(["nested"], { type: "text/plain" }));

      await synchronizer.synchronizeDirectory("/folder", false);

      const localMeta = await localFE.getMetadata();
      const remoteParentDE = await remote.root.getDirectory("folder");
      const remoteFE = await remoteParentDE.getFile("fuga.txt");
      const remoteMeta = await remoteFE.getMetadata();
      expect(remoteMeta.size).toBe(localMeta.size);
      const remoteDE = await remoteParentDE.getDirectory("nested");
      expect(remoteDE.fullPath).toBe(localDE.fullPath);
      try {
        await remoteDE.getFile("nested.txt");
      } catch (e) {
        expect(e).toBeInstanceOf(NotFoundError);
      }
    } catch (e) {
      fail(e);
    } finally {
      done();
    }
  });

  test("sync dir recursively", async (done) => {
    try {
      await synchronizer.synchronizeDirectory("/folder", true);

      const localNestedFE = await local.root.getFile(
        "/folder/nested/nested.txt"
      );
      const localNestedMeta = await localNestedFE.getMetadata();
      const remoteParentDE = await remote.root.getDirectory("folder");
      const remoteDE = await remoteParentDE.getDirectory("nested");
      const remoteNestedFE = await remoteDE.getFile("nested.txt");
      const remoteNestedMeta = await remoteNestedFE.getMetadata();
      expect(remoteNestedMeta.size).toBe(localNestedMeta.size);
    } catch (e) {
      fail(e);
    } finally {
      done();
    }
  });

  test("remove file, sync all", async (done) => {
    try {
      let localFE = await local.root.getFile("empty.txt");
      await localFE.remove();

      await synchronizer.synchronizeAll();

      try {
        await remote.root.getFile("/empty.txt");
      } catch (e) {
        expect(e).toBeInstanceOf(NotFoundError);
      }
    } catch (e) {
      fail(e);
    } finally {
      done();
    }
  });
}
