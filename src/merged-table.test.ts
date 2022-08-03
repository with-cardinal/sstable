import fs from "fs/promises";
import path from "path";
import { MergedTable } from "./merged-table";
import { Table } from "./table";
import { TableBuilder } from "./table-builder";

test("shows consistent view from multiple tables at once", async () => {
  const testDir = await fs.mkdtemp("tmp/test");

  const builders = [
    new TableBuilder(path.join(testDir, "table1.sstable")),
    new TableBuilder(path.join(testDir, "table2.sstable")),
    new TableBuilder(path.join(testDir, "table3.sstable")),
  ];

  for (let t = 0; t < builders.length; t++) {
    for (let i = t; i < 500; i = i + 3) {
      await builders[t].add(
        Buffer.from(`key${i.toString().padStart(10, "0")}`),
        Buffer.from("a".repeat(i))
      );
    }
  }
  for (const b of builders) {
    await b.close();
  }

  const tables = [
    new Table(path.join(testDir, "table1.sstable")),
    new Table(path.join(testDir, "table2.sstable")),
    new Table(path.join(testDir, "table3.sstable")),
  ];
  const merged = new MergedTable(tables);

  for (let i = 0; i < 500; i = i + 30) {
    expect(
      await merged.get(Buffer.from(`key${i.toString().padStart(10, "0")}`))
    ).toEqual(Buffer.from("a".repeat(i)));
  }

  const cursor = await merged.cursor();

  await cursor.seek(Buffer.from("key0000000250"));
  let entry = await cursor.next();
  let counter = 1;
  while (entry) {
    counter++;
    entry = await cursor.next();
  }
  expect(counter).toEqual(250);

  for (const t of tables) {
    await t.close();
  }

  await fs.rm(testDir, { recursive: true });
});
