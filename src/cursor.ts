export interface Cursor {
  peek(): Promise<[Buffer, Buffer] | undefined>;
  next(): Promise<[Buffer, Buffer] | undefined>;
  seek(key: Buffer): Promise<void>;
}
