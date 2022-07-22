import { mkdtemp, rm } from "fs/promises";
import path from "path";
import { TableBuilder } from "./table-builder";

let testDir: string;
beforeAll(async () => {
  testDir = await mkdtemp("tmp/test");
});

afterAll(async () => {
  await rm(testDir, { recursive: true });
});

test("add out of order", async () => {
  const tablePath = path.join(testDir, "out-of-order.sstable");
  const b = new TableBuilder(tablePath);

  await b.add(Buffer.from("b"), Buffer.from("b"));
  await expect(async () =>
    b.add(Buffer.from("a"), Buffer.from("a"))
  ).rejects.toThrow();

  await b.close();
});
