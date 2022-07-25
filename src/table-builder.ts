import fs, { FileHandle } from "node:fs/promises";
import { BlockBuilder } from "./block";
import assert from "node:assert";

export class TableBuilder {
  private path: string;
  private handle?: FileHandle;

  private offset = 0;
  private blockFirstKeys: [Buffer, number][] = [];

  private blockFirstKey?: Buffer;
  private blockBuilder?: BlockBuilder;

  private previousKey?: Buffer;

  constructor(path: string) {
    this.path = path;
  }

  async add(key: Buffer, value: Buffer): Promise<void> {
    if (this.previousKey && this.previousKey?.compare(key) > 0) {
      throw new Error("added out of order");
    }

    if (!this.handle) {
      this.handle = await fs.open(this.path, "wx");
    }

    if (!this.blockBuilder) {
      this.blockBuilder = new BlockBuilder();
    }

    const res = await this.blockBuilder.add(key, value);
    if (!res) {
      await this.writeCurrentBlock();
      await this.blockBuilder.add(key, value);
    }

    if (!this.blockFirstKey) {
      this.blockFirstKey = key;
    }

    this.previousKey = key;
  }

  // write the current block builder and start a new one
  private async writeCurrentBlock() {
    assert.ok(this.blockBuilder, "Invalid state - write with no BlockBuilder");
    assert.ok(this.handle, "Invalid state - write with no handle");
    assert.ok(this.blockFirstKey, "Invalid state - write with no keys");

    const out = this.blockBuilder.close();
    await this.handle.write(out);
    this.blockFirstKeys.push([this.blockFirstKey, this.offset]);
    this.offset = this.offset + out.byteLength;
    this.blockFirstKey = undefined;
    this.blockBuilder = new BlockBuilder();
  }

  async close() {
    assert.ok(this.handle, "Invalid state - write with no handle");

    // write the last block if it has any keys
    if (this.blockBuilder && this.blockFirstKey) {
      await this.writeCurrentBlock();
    }

    assert.ok(
      this.blockFirstKeys.length > 0,
      "Invalid sstable - no records stored"
    );

    // write the metadata blocks
    const metadataOffsets: number[] = [];
    let idxBlockBuilder: BlockBuilder | undefined;

    for (const [blockKey, blockOffset] of this.blockFirstKeys) {
      const offsetBuf = Buffer.alloc(8);
      offsetBuf.writeUIntBE(blockOffset, 2, 6);

      if (
        !idxBlockBuilder ||
        !(await idxBlockBuilder.add(blockKey, offsetBuf))
      ) {
        if (idxBlockBuilder) {
          const block = idxBlockBuilder.close();
          idxBlockBuilder = undefined;

          metadataOffsets.push(this.offset);
          await this.handle.write(block);
          this.offset = this.offset + block.byteLength;
        }

        idxBlockBuilder = new BlockBuilder();
        await idxBlockBuilder.add(blockKey, offsetBuf);
      }
    }

    // write trailing block builder
    if (idxBlockBuilder) {
      const block = idxBlockBuilder.close();
      idxBlockBuilder = undefined;

      metadataOffsets.push(this.offset);
      await this.handle.write(block);
      this.offset = this.offset + block.byteLength;
    }

    // write the trailer
    const trailer = Buffer.alloc(metadataOffsets.length * 6 + 4);
    for (let i = 0; i < metadataOffsets.length; i++) {
      trailer.writeUIntBE(metadataOffsets[i], i * 6, 6);
    }

    trailer.writeUInt32BE(metadataOffsets.length, metadataOffsets.length * 6);
    await this.handle.write(trailer);

    await this.handle.close();
  }
}
