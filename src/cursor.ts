export interface Cursor {
  next(): Promise<[Buffer, Buffer] | undefined>;
  seek(key: Buffer): Promise<void>;
}
