import { BlockBuilder, Block, BLOCK_SIZE_TARGET } from "./block";

describe("block", () => {
  test("empty block", () => {
    const builder = new BlockBuilder();
    expect(builder.close()).toBeTruthy();
  });

  test("don't allow insert out of order", async () => {
    const b = new BlockBuilder();
    await b.add(Buffer.from("b"), Buffer.from("b"));
    await expect(
      async () => await b.add(Buffer.from("a"), Buffer.from("a"))
    ).rejects.toThrow();
  });

  test("accept any size first record", async () => {
    const builder = new BlockBuilder();
    const res = await builder.add(
      Buffer.from("key"),
      Buffer.from("a".repeat(BLOCK_SIZE_TARGET + 1))
    );

    expect(res).toBe(true);
  });

  test("rejects overflow", async () => {
    const builder = new BlockBuilder();
    const resA = await builder.add(Buffer.from("a"), Buffer.from("a"));
    expect(resA).toBe(true);

    const resB = await builder.add(
      Buffer.from("b"),
      Buffer.from("b".repeat(BLOCK_SIZE_TARGET + 1))
    );
    expect(resB).toBe(false);
  });

  test("build and read", async () => {
    const builder = new BlockBuilder();
    await builder.add(Buffer.from("key1"), Buffer.from("1".repeat(1000)));
    await builder.add(Buffer.from("key2"), Buffer.from("2".repeat(1000)));
    await builder.add(Buffer.from("key3"), Buffer.from("3".repeat(1000)));
    const outBuf = builder.close();

    const block = await Block.fromBuffer(outBuf);
    expect(block.get(Buffer.from("key1"))).toEqual(
      Buffer.from("1".repeat(1000))
    );
  });
});
