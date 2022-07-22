import assert from "node:assert";
import fs, { FileHandle } from "node:fs/promises";
import { Block } from "./block";

type BlockEntryTable = [Buffer, number][];

function blockIdx(blockEntries: BlockEntryTable, key: Buffer): number {
  return blockEntries
    .slice()
    .reverse()
    .findIndex(([entryKey]) => entryKey.compare(key) <= 0);
}

async function readBlock(
  handle: FileHandle,
  entries: BlockEntryTable,
  blocksEnd: number,
  idx: number
): Promise<Block> {
  const [start, end] = [
    entries[idx][1],
    idx + 1 === entries.length ? blocksEnd : entries[idx + 1][1],
  ];

  const blockBuf = Buffer.alloc(end - start);
  assert.ok(
    (await handle.read(blockBuf, 0, blockBuf.byteLength, start)).bytesRead ===
      blockBuf.byteLength
  );

  return new Block(blockBuf);
}

export class Table {
  private path: string;
  private handle?: FileHandle;

  blockEntries: [Buffer, number][] = [];
  blockEntriesEnd = 0;

  constructor(path: string) {
    this.path = path;
  }

  private async ensureOpen() {
    if (!this.handle) {
      this.handle = await fs.open(this.path, "r");
    }

    const stat = await this.handle.stat();
    assert.ok(stat.size > 4, "Invalid table file");

    // read number of meta blocks
    const sizeBuf = Buffer.alloc(4);

    assert.equal(
      (await this.handle.read(sizeBuf, 0, 4, stat.size - 4)).bytesRead,
      4,
      "metadata block count read fewer bytes than expected"
    );
    const metadataBlockCount = sizeBuf.readUInt32BE(0);

    // read offsets of meta blocks
    const blockRefBuf = Buffer.alloc(metadataBlockCount * 6);
    const metaBlockPos = stat.size - metadataBlockCount * 6 - 4;
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

      new Block(blockBuf).entries.map(([key, value]) => {
        const offset = value.readUInt32BE(0);
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
      keyBlockIdx
    );
    return block.get(key);
  }

  async cursor(): Promise<Cursor> {
    await this.ensureOpen();
    return new Cursor(this.handle, this.blockEntries, this.blockEntriesEnd);
  }

  async close() {
    this.handle?.close();
  }
}

class Cursor {
  private handle: FileHandle;
  private entries: BlockEntryTable;
  private entriesEnd: number;

  private currentBlockIndex = 0;
  private currentBlock?: Block;
  private currentBlockOffset = 0;

  constructor(
    handle: FileHandle | undefined,
    entries: BlockEntryTable,
    entriesEnd: number
  ) {
    assert.ok(handle !== undefined, "Error: invalid file handle");
    this.handle = handle;
    this.entries = entries;
    this.entriesEnd = entriesEnd;
  }

  private async ensureBlock() {
    if (!this.currentBlock) {
      this.currentBlock = await readBlock(
        this.handle,
        this.entries,
        this.entriesEnd,
        this.currentBlockIndex
      );
    }
  }

  async next(): Promise<[Buffer, Buffer] | undefined> {
    await this.ensureBlock();

    assert.ok(this.currentBlock !== undefined, "Error: invalid block");

    // move to next block if at the end of the current block
    if (this.currentBlockOffset >= this.currentBlock.entries.length) {
      this.currentBlockOffset = 0;
      this.currentBlockIndex++;
      await this.ensureBlock();
    }

    // if at the end of the table
    if (this.currentBlockIndex >= this.entries.length) {
      return undefined;
    }

    const [key, value] = this.currentBlock.entries[this.currentBlockOffset];
    this.currentBlockOffset++;
    return [key, value];
  }
}
