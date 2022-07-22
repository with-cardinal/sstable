import { mkdtemp, rm } from "fs/promises";
import path from "path";
import { TableBuilder } from "./table-builder";
import { Table } from "./table";

let testDir: string;
let tablePath: string;
const recordCount = 100;

beforeAll(async () => {
  testDir = await mkdtemp("tmp/test");

  tablePath = path.join(testDir, "table.sstable");
  const b = new TableBuilder(tablePath);

  for (let i = 0; i < recordCount; i++) {
    await b.add(
      Buffer.from(`key${i.toString().padStart(10, "0")}`),
      Buffer.from("a".repeat(i))
    );
  }

  await b.close();
});

afterAll(async () => {
  await rm(testDir, { recursive: true });
});

test("build and access", async () => {
  const t = new Table(tablePath);

  const c = await t.cursor();
  let counter = 0;
  let done = false;
  while (!done) {
    const next = await c.next();
    if (next) {
      counter++;
    } else {
      done = true;
    }
  }

  await t.close();

  expect(counter).toBe(recordCount);
});
