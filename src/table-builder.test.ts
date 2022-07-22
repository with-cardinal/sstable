import { mkdtemp, rm } from "fs/promises";
import path from "path";
import { TableBuilder } from "./table-builder";
import { Table } from "./table";

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

test("build and access", async () => {
  const tablePath = path.join(testDir, "table-build.sstable");
  const b = new TableBuilder(tablePath);

  for (let i = 0; i < 100; i++) {
    await b.add(
      Buffer.from(`key${i.toString().padStart(10, "0")}`),
      Buffer.from("a".repeat(i))
    );
  }

  await b.close();

  const t = new Table(tablePath);
  const value = await t.get(Buffer.from(`key${"0".padStart(10, "0")}`));
  expect(value).toEqual(Buffer.from("a".repeat(0)));

  await t.close();
});
