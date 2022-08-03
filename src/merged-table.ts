import { Table } from "./table";
import { Cursor } from "./cursor";
import assert from "assert";

export class MergedTable {
  private _tables: Table[];

  constructor(tables: Table[]) {
    this._tables = tables;
  }

  async get(key: Buffer): Promise<Buffer | undefined> {
    const results = await Promise.all(this._tables.map((t) => t.get(key)));
    return results.find((r) => r !== undefined);
  }

  async cursor(): Promise<Cursor> {
    const cursors = await Promise.all(this._tables.map((t) => t.cursor()));
    return new MergedCursor(cursors);
  }
}

class MergedCursor {
  private _cursors: Cursor[];

  constructor(cursors: Cursor[]) {
    this._cursors = cursors;
  }

  private async nextCursorIndex(): Promise<number> {
    const peeks = await Promise.all(this._cursors.map((c) => c.peek()));

    // read the lowest value
    const filteredKeys = peeks.filter(
      (val): val is [Buffer, Buffer] => val !== undefined
    );

    if (filteredKeys.length === 0) {
      return -1;
    }

    const sortedKeys = filteredKeys
      .map((val) => val[0])
      .sort((a, b) => a.compare(b));
    const minKey = sortedKeys[0];

    const minIndex = filteredKeys.findIndex((val) => val[0].equals(minKey));
    assert.ok(minIndex !== -1, "Unexpected error: index not found");

    return minIndex;
  }

  async peek(): Promise<[Buffer, Buffer] | undefined> {
    const nextIndex = await this.nextCursorIndex();
    if (nextIndex === -1) {
      return undefined;
    } else {
      return await this._cursors[nextIndex].next();
    }
  }

  async next(): Promise<[Buffer, Buffer] | undefined> {
    const nextIndex = await this.nextCursorIndex();
    if (nextIndex === -1) {
      return undefined;
    } else {
      return await this._cursors[nextIndex].next();
    }
  }

  async seek(key: Buffer): Promise<void> {
    await Promise.all(this._cursors.map((c) => c.seek(key)));
  }
}
