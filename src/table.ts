import assert from "node:assert";
import fs, { FileHandle } from "node:fs/promises";
import { Block } from "./block";

interface BlockEntries {
  blockEntries: [Buffer, number][];
  blockEntriesEnd: number;
}

function blockIdx(be: BlockEntries, key: Buffer): number {
  return be.blockEntries
    .slice()
    .reverse()
    .findIndex(([entryKey]) => entryKey.compare(key) <= 0);
}

function blockRange(be: BlockEntries, idx: number): [number, number] {
  return [
    be.blockEntries[idx][1],
    idx + 1 === be.blockEntries.length
      ? be.blockEntriesEnd
      : be.blockEntries[idx + 1][1],
  ];
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

    const keyBlockIdx = blockIdx(this, key);
    if (keyBlockIdx === -1) {
      return undefined;
    }

    const [blockStart, blockEnd] = blockRange(this, keyBlockIdx);
    const blockBuf = Buffer.alloc(blockEnd - blockStart);
    await this.handle?.read(blockBuf, 0, blockBuf.byteLength, blockStart);

    const block = new Block(blockBuf);
    return block.get(key);
  }

  async close() {
    this.handle?.close();
  }
}
