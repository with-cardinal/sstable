import { TableBuilder } from "./table-builder";

export class Memtable {
  private _map = new Map<string, Buffer>();
  private _byteLength = 0;

  get byteLength(): number {
    return this._byteLength;
  }

  get size(): number {
    return Array.from(this._map.keys()).length;
  }

  put(key: Buffer, value: Buffer): void {
    const keyString = key.toString();

    if (this._map.has(keyString)) {
      this._byteLength -=
        key.byteLength + (this._map.get(keyString)?.byteLength || 0);
    }

    this._map.set(keyString, value);
    this._byteLength += key.byteLength + value.byteLength;
  }

  get(key: Buffer): Buffer | undefined {
    return this._map.get(key.toString());
  }

  async save(path: string): Promise<void> {
    const entries = Array.from(this._map.entries());
    const sorted = entries.sort((a, b) => (a[0] < b[0] ? -1 : 1));

    const tb = new TableBuilder(path);

    for (const [key, value] of sorted) {
      await tb.add(Buffer.from(key), value);
    }

    await tb.close();
  }
}
