import { Buffer } from "node:buffer";
import assert from "node:assert";
import { compress, compressSync, uncompress, uncompressSync } from "snappy";

export const BLOCK_SIZE_TARGET = 131072;
const FIELD_LIMIT = 4294967295;
const SHARED_SIZE_OFFSET = 0;
const KEY_SIZE_OFFSET = 4;
const VALUE_SIZE_OFFSET = 8;
const KEY_OFFSET = 12;
const RECORD_OVERHEAD = 12;

const COMPRESSION_ASYNC_THRESHOLD = 32768;

export class BlockBuilder {
  // the block's data
  records: Buffer[] = [];

  // bytes added so far
  byteLength = 0;

  previousKey?: Buffer;

  // generate the bytes for a record
  private async buildRecord(key: Buffer, value: Buffer): Promise<Buffer> {
    assert.ok(key.byteLength <= FIELD_LIMIT, "key length exceeds limit");
    assert.ok(value.byteLength <= FIELD_LIMIT, "value length exceeds limit");

    let compressedValue;
    if (value.byteLength > COMPRESSION_ASYNC_THRESHOLD) {
      compressedValue = await compress(value);
    } else {
      compressedValue = compressSync(value);
    }

    const out = Buffer.alloc(key.byteLength + compressedValue.byteLength + 12);

    // shared prefix - future optimization
    out.writeUInt32BE(0, SHARED_SIZE_OFFSET);
    out.writeUInt32BE(key.byteLength, KEY_SIZE_OFFSET);
    out.writeUInt32BE(compressedValue.byteLength, VALUE_SIZE_OFFSET);

    key.copy(out, KEY_OFFSET);
    compressedValue.copy(out, KEY_OFFSET + key.byteLength);

    return out;
  }

  // return true if block is added, false otherwise
  async add(key: Buffer, value: Buffer): Promise<boolean> {
    if (this.previousKey && this.previousKey?.compare(key) > 0) {
      throw new Error("added out of order");
    }

    if (
      this.byteLength > 0 &&
      this.byteLength + key.byteLength + value.byteLength + RECORD_OVERHEAD >
        BLOCK_SIZE_TARGET
    ) {
      return false;
    }

    const record = await this.buildRecord(key, value);
    this.records.push(record);
    this.byteLength += record.byteLength;
    this.previousKey = key;

    return true;
  }

  close(): Buffer {
    const trailer = Buffer.alloc(4);
    trailer.writeUInt32BE(0, 0);

    return Buffer.concat([...this.records, trailer]);
  }
}

export class Block {
  private _entries: [Buffer, Buffer][] = [];

  static async fromBuffer(buf: Buffer): Promise<Block> {
    const entries: [Buffer, Buffer][] = [];
    const exclTrailer = buf.subarray(0, -4);

    let offset = 0;
    while (offset < exclTrailer.byteLength) {
      const keySize = exclTrailer.readUInt32BE(offset + KEY_SIZE_OFFSET);
      const valueSize = exclTrailer.readUInt32BE(offset + VALUE_SIZE_OFFSET);
      const key = exclTrailer.slice(
        offset + KEY_OFFSET,
        offset + KEY_OFFSET + keySize
      );
      const valueEnd = offset + KEY_OFFSET + keySize + valueSize;

      const compressedValue = exclTrailer.slice(
        offset + KEY_OFFSET + keySize,
        valueEnd
      );

      if (valueSize > COMPRESSION_ASYNC_THRESHOLD) {
        const value = (await uncompress(compressedValue, {
          asBuffer: true,
        })) as Buffer;
        entries.push([key, value]);
      } else {
        const value = uncompressSync(compressedValue, {
          asBuffer: true,
        }) as Buffer;

        entries.push([key, value]);
      }
      offset = valueEnd;
    }

    return new Block(entries);
  }

  constructor(entries: [Buffer, Buffer][]) {
    this._entries = entries;
  }

  get(key: Buffer): Buffer | undefined {
    const entry = this._entries.find((entry) => entry[0].equals(key));
    if (!entry) {
      return undefined;
    }

    return entry[1];
  }

  get keys(): Buffer[] {
    return this._entries.map((entry) => entry[0]);
  }

  get entries(): [Buffer, Buffer][] {
    return this._entries;
  }
}
