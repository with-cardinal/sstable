import { Cursor } from "./cursor";
import assert from "node:assert";
import { BlockTable, readBlock, blockIdx } from "./table";
import { FileHandle } from "node:fs/promises";
import { Block } from "./block";

export class TableCursor implements Cursor {
  private handle: FileHandle;
  private blocks: BlockTable;
  private blocksEnd: number;

  private currentBlockIndex = 0;
  private currentBlock?: Block;
  private currentBlockOffset = 0;

  constructor(
    handle: FileHandle | undefined,
    blocks: BlockTable,
    blocksEnd: number
  ) {
    assert.ok(handle !== undefined, "Error: invalid file handle");
    this.handle = handle;
    this.blocks = blocks;
    this.blocksEnd = blocksEnd;
  }
  close(): void {
    throw new Error("Method not implemented.");
  }

  private async ensureBlock() {
    if (!this.currentBlock) {
      this.currentBlock = await readBlock(
        this.handle,
        this.blocks,
        this.blocksEnd,
        this.currentBlockIndex
      );
    }
  }

  async next(): Promise<[Buffer, Buffer] | undefined> {
    if (this.currentBlockIndex >= this.blocks.length) {
      return undefined;
    }

    await this.ensureBlock();
    assert.ok(this.currentBlock !== undefined, "Error: invalid block");

    // move to next block if at the end of the current block
    if (this.currentBlockOffset >= this.currentBlock.entries.length) {
      this.currentBlockOffset = 0;
      this.currentBlockIndex++;
      this.currentBlock = undefined;
    }

    // if at the end of the table
    if (this.currentBlockIndex >= this.blocks.length) {
      return undefined;
    }

    await this.ensureBlock();
    assert.ok(this.currentBlock !== undefined, "Error: invalid block");

    if (this.currentBlockOffset >= this.currentBlock.entries.length) {
      return undefined;
    }

    const [key, value] = this.currentBlock.entries[this.currentBlockOffset];
    this.currentBlockOffset++;
    return [key, value];
  }

  async seek(key: Buffer): Promise<void> {
    const blockIndex = blockIdx(this.blocks, key);

    // no block found, just set to end of table
    if (blockIndex < 0) {
      this.currentBlockIndex = this.blocks.length;
      this.currentBlockOffset = 0;
      return;
    }

    this.currentBlockIndex = blockIndex;
    this.currentBlockOffset = 0;

    await this.ensureBlock();
    assert.ok(this.currentBlock !== undefined, "Error: invalid block");

    // find first entry greater than or equal to key
    const idx = this.currentBlock?.entries.findIndex(
      ([entryKey]) => entryKey.compare(key) >= 0
    );

    if (idx < 0) {
      this.currentBlockOffset = this.currentBlock?.entries.length;
    } else {
      this.currentBlockOffset = idx;
    }
  }
}
