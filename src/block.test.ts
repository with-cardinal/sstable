import { BlockBuilder, Block, BLOCK_SIZE_TARGET } from "./block";

describe("block", () => {
  test("empty block", () => {
    const builder = new BlockBuilder();
    expect(builder.close()).toBeTruthy();
  });

  test("don't allow insert out of order", () => {
    const b = new BlockBuilder();
    b.add(Buffer.from("b"), Buffer.from("b"));
    expect(() => b.add(Buffer.from("a"), Buffer.from("a"))).toThrow();
  });

  test("accept any size first record", () => {
    const builder = new BlockBuilder();
    const res = builder.add(
      Buffer.from("key"),
      Buffer.from("a".repeat(BLOCK_SIZE_TARGET + 1))
    );

    expect(res).toBe(true);
  });

  test("rejects overflow", () => {
    const builder = new BlockBuilder();
    const resA = builder.add(Buffer.from("a"), Buffer.from("a"));
    expect(resA).toBe(true);

    const resB = builder.add(
      Buffer.from("b"),
      Buffer.from("b".repeat(BLOCK_SIZE_TARGET + 1))
    );
    expect(resB).toBe(false);
  });

  test("build and read", () => {
    const builder = new BlockBuilder();
    builder.add(Buffer.from("key1"), Buffer.from("1".repeat(1000)));
    builder.add(Buffer.from("key2"), Buffer.from("2".repeat(1000)));
    builder.add(Buffer.from("key3"), Buffer.from("3".repeat(1000)));
    const outBuf = builder.close();

    const block = new Block(outBuf);
    expect(block.get(Buffer.from("key1"))).toEqual(
      Buffer.from("1".repeat(1000))
    );
  });
});
