import assert from "node:assert";
import fs, { FileHandle } from "node:fs/promises";
import { Block } from "./block";
import { Cursor } from "./cursor";
import { TableCursor } from "./table-cursor";

export type BlockTable = [Buffer, number][];

const BLOCK_CACHE_LIMIT = 4;

export function blockIdx(blockEntries: BlockTable, key: Buffer): number {
  const idx = blockEntries
    .slice()
    .reverse()
    .findIndex(([entryKey]) => entryKey.compare(key) <= 0);

  if (idx < 0) {
    return idx;
  }

  return blockEntries.length - 1 - idx;
}

export async function readBlock(
  handle: FileHandle,
  entries: BlockTable,
  blocksEnd: number,
  idx: number,
  cache: [number, Block][] = []
): Promise<Block> {
  // use the cache if it's available
  if (cache.length > 0) {
    const found = cache.findIndex(([i]) => i === idx);

    if (found !== -1) {
      return cache[found][1];
    }
  }

  const [start, end] = [
    entries[idx][1],
    idx + 1 === entries.length ? blocksEnd : entries[idx + 1][1],
  ];

  const blockBuf = Buffer.alloc(end - start);
  assert.ok(
    (await handle.read(blockBuf, 0, blockBuf.byteLength, start)).bytesRead ===
      blockBuf.byteLength
  );

  const block = await Block.fromBuffer(blockBuf);

  cache.push([idx, block]);
  if (cache.length > BLOCK_CACHE_LIMIT) {
    cache.splice(0, cache.length - BLOCK_CACHE_LIMIT);
  }

  return block;
}

export class Table {
  private path: string;
  private handle?: FileHandle;

  private blockEntries: [Buffer, number][] = [];
  private blockEntriesEnd = 0;

  private cache: [number, Block][] = [];

  constructor(path: string) {
    this.path = path;
  }

  private async ensureOpen() {
    if (this.handle) {
      return;
    }

    this.handle = await fs.open(this.path, "r");

    const stat = await this.handle.stat();
    assert.ok(stat.size > 4, "Invalid table file");

    // read number of meta blocks
    const sizeBuf = Buffer.alloc(4);
    assert.equal(
      (await this.handle.read(sizeBuf, 0, 4, stat.size - 4)).bytesRead,
      4,
      "metadata block count read fewer bytes than expected"
    );
    const metadataBlockCount = sizeBuf.readUInt32BE();

    // read offsets of meta blocks
    const blockRefBuf = Buffer.alloc(metadataBlockCount * 6);
    const trailerLength = metadataBlockCount * 6 + 4;
    const metaBlockPos = stat.size - trailerLength;

    assert.equal(
      (
        await this.handle.read(
          blockRefBuf,
          0,
          blockRefBuf.byteLength,
          metaBlockPos
        )
      ).bytesRead,
      metadataBlockCount * 6,
      "Block ref read fewer bytes than expected"
    );

    const metaBlockOffsets: number[] = [];
    for (let i = 0; i < metadataBlockCount; i++) {
      metaBlockOffsets[i] = blockRefBuf.readUIntBE(i * 6, 6);
    }

    // read each meta block
    for (let i = 0; i < metaBlockOffsets.length; i++) {
      const blockStart = metaBlockOffsets[i];
      const blockEnd =
        i < metaBlockOffsets.length - 1
          ? metaBlockOffsets[i + 1]
          : metaBlockPos;
      const blockBuf = Buffer.alloc(blockEnd - blockStart);

      assert(
        (await (
          await this.handle.read(blockBuf, 0, blockBuf.byteLength, blockStart)
        ).bytesRead) === blockBuf.byteLength,
        "Block read fewer bytes than expected"
      );

      const metaBlock = await Block.fromBuffer(blockBuf);
      metaBlock.entries.map(([key, value]) => {
        const offset = value.readIntBE(2, 6);
        this.blockEntries.push([key, offset]);
      });
      this.blockEntriesEnd = metaBlockOffsets[0];
    }
  }

  // get finds the given key, or undefined if it's not there
  async get(key: Buffer): Promise<Buffer | undefined> {
    await this.ensureOpen();

    const keyBlockIdx = blockIdx(this.blockEntries, key);
    if (keyBlockIdx === -1) {
      return undefined;
    }

    assert.ok(this.handle !== undefined, "Error: invalid file handle");
    const block = await readBlock(
      this.handle,
      this.blockEntries,
      this.blockEntriesEnd,
      keyBlockIdx,
      this.cache
    );
    return block.get(key);
  }

  async cursor(): Promise<Cursor> {
    await this.ensureOpen();
    return new TableCursor(
      this.handle,
      this.blockEntries,
      this.blockEntriesEnd
    );
  }

  async close() {
    this.handle?.close();
  }
}
