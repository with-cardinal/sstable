# sstable

Sorted strings tables for Node.js

## Installation

```
npm install @withcardinal/sstable
```

## Usage

An sstable, or sorted strings table, is a simple file format that records key value pairs, sorted by key.

### Writing an sstable file

Use a TableBuilder to write an sstable file. Records must be added in sorted
order.

```javascript
// open a table builder
const tb = new TableBuilder("animals.sstable");

// add records
await tb.add(
  Buffer.from("Bearded Dragon"),
  Buffer.from(JSON.stringify({ kingdom: "mammal" }))
);
await tb.add(
  Buffer.from("Dog"),
  Buffer.from(JSON.stringify({ kingdom: "mammal" }))
);

// close the table
await tb.close();
```

Once you have an sstable file you can open and look up keys in it with `Table`:

```javascript
const table = new Table("animals.sstable");

// print the Dog record (as a buffer)
const value = await table.get(Buffer.from("Dog"));
console.log(value?.toString());

// close it
await table.close();
```

Tables can be searched and iterated with cursors:

```javascript
const table = new Table("animals.sstable");

// iterate all keys
const cursor = await table.cursor();
await cursor.seek(Buffer.from("Dog"));

for (
  let animal = await cursor.next();
  animal !== undefined;
  animal = await cursor.next()
) {
  console.log(`key=${animal[0].toString()}, value=${animal[1].toString()}`);
}

// close it
await table.close();
```

# API Documentation

## `TableBuilder`

### `constructor(path: string)`

Construct a new `Table Builder`.

#### Parameters

- `path` - the file path to write the new sstable to. No file should exist at this path.

### `add(key: Buffer, value: Buffer) : Promise<void>`

Add a new key/value pair to the table. Keys must be added in sorted order.

#### Parameters

- `key` - The key to add. 
- `value` - The value to add

#### Throws

- Keys must added in sorted order. If `key` is less than the previously added key an error will be thrown.
- Files are lazily opened once the first key is added. If a file already exists at `path` an error will be thrown.

### `close() : Promise<void>`

Flush final data and close the `TableBuilder`.

## `Table`

### `constructor(path: string)`

Construct a new table from the file at `path`.

### `get(key: Buffer) : Promise<Buffer | undefined>`

Read the value at `key`, or return `undefined` if the key does not exist.

#### Parameters

- `key` - the key to look up

#### Returns

- `Buffer` if the key is found, `undefined` otherwise

#### Throws

- Files are lazily opened, so can throw if the file at `path` isn't found.

### `cursor() : Promise<Cursor>`

Returns a new `Cursor` for iterating the `Table` from the start.

#### Returns

- A new cursor

#### Throws

- Files are lazily opened, so can throw if the file at `path` isn't found.

### `close() : Promise<void>`

Closes the `Table` and the underlying file.

## `Cursor`

Returned by calling `cursor` on a `Table` or `MergedTable` instance.

### `peek() : Promise<[Buffer, Buffer] | undefined>`

Peeks the next value in the sstable

### `next() : Promise<[Buffer, Buffer] | undefined>`

Reads the next value in the sstable and returns its key and value.

#### Returns

- Returns an array of two buffers, `[key, value]` if a next entry is available, or `undefined` otherwise.

### `seek(key: Buffer) : Promise<void>`

Seek to `key` in the sstable. If `key` doesn't exist, seek to the position prior to the next value greater than `key`.

#### Parameters

- `key` - the position to seek to in the sstable. If `key` doesn't exist in the table the seek will go to just prior to the next key greater than `key`.

## `MergedTable`

### `constructor(tables: Table[])`

Construct a new `MergedTable` from a set of tables. The `MergedTable` will 
provide a merged view of all the tables at once, giving priority to lower 
indexed tables in the `tables` array.

Because `MergedTable` objects are composed of multiple `Table` objects, be sure
to close all of the `Table` objects after you're done with the `MergedTable`.

### `get(key: Buffer): Promise<Buffer | undefined>`

Read the value at `key`, or return `undefined` if the key does not exist.

#### Parameters

- `key` - the key to look up

#### Returns

- `Buffer` if the key is found, `undefined` otherwise

### `cursor() : Promise<Cursor>`

Returns a new `Cursor` for iterating the `MergedTable` from the start.

#### Returns

- A new cursor

## License

MIT. See LICENSE for details.