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
  private _nextVals: ([Buffer, Buffer] | undefined)[];

  constructor(cursors: Cursor[]) {
    this._cursors = cursors;
    this._nextVals = new Array(cursors.length);
  }

  async next(): Promise<[Buffer, Buffer] | undefined> {
    // fill up next vals
    const promises: Promise<void>[] = [];
    for (let i = 0; i < this._nextVals.length; i++) {
      if (!this._nextVals[i]) {
        promises.push(
          (async () => {
            this._nextVals[i] = await this._cursors[i].next();
          })()
        );
      }
    }
    await Promise.all(promises);

    // read the lowest value
    const filteredKeys = this._nextVals.filter(
      (val): val is [Buffer, Buffer] => val !== undefined
    );

    if (filteredKeys.length === 0) {
      return undefined;
    }

    const sortedKeys = filteredKeys
      .map((val) => val[0])
      .sort((a, b) => a.compare(b));
    const minKey = sortedKeys[0];

    const minIndex = filteredKeys.findIndex((val) => val[0].equals(minKey));
    assert.ok(minIndex !== -1, "Unexpected error: index not found");

    const out = this._nextVals[minIndex];
    this._nextVals[minIndex] = undefined;

    this._nextVals.map((val) => {
      if (val !== undefined) {
        if (val[0].compare(minKey) === 0) {
          return undefined;
        }

        return val;
      }

      return undefined;
    });

    return out;
  }

  async seek(key: Buffer): Promise<void> {
    await Promise.all(this._cursors.map((c) => c.seek(key)));
  }
}
