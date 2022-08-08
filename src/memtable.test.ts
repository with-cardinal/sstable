import { Memtable } from "./memtable";
import fs from "fs/promises";
import path from "path";
import { Table } from "./table";

test("basic usage", () => {
  const mt = new Memtable();

  const iterations = 1000;
  for (let i = 0; i < iterations; i++) {
    mt.add(Buffer.from(`key${i}`), Buffer.from(`value${i}`));
  }

  for (let i = 0; i < iterations; i++) {
    expect(mt.get(Buffer.from(`key${i}`))).not.toBeUndefined();
  }

  expect(mt.size).toBe(1000);
  expect(mt.byteLength).toBe(13780);
});

test("save", async () => {
  const testDir = await fs.mkdtemp("tmp/test");

  const mt = new Memtable();
  const iterations = 1000;
  for (let i = 0; i < iterations; i++) {
    mt.add(
      Buffer.from(i.toString().padStart(4, "0")),
      Buffer.from(`value${i}`)
    );
  }

  await mt.save(path.join(testDir, "memtable.sstable"));

  const table = new Table(path.join(testDir, "memtable.sstable"));
  const cursor = await table.cursor();

  let entry = await cursor.next();
  let counter = 0;
  while (entry) {
    counter++;
    entry = await cursor.next();
  }

  expect(counter).toBe(iterations);

  await table.close();
  await fs.rm(testDir, { recursive: true });
});
