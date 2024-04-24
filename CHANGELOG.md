# CHANGELOG

## 0.1.12

- added KV workload.
- SimpleFaker: fixed representation of ARRAY objects.
- added execution of `setup()` for any newly created thread.
- added ID for threads. ID and total thread count are passed to the `setup()` function.

## 0.1.11

- SimpleFaker: added `prefix` to `string` generator.
- Gracefully exit on unknown datatype.
- Added CLOB as STRING alias.
- Fixed `Float` generator bug that created numbers bigger than desired DECIMAL precision.
- Fixed `IMPORT INTO` statement created by `util csv` command.
- SimpleFaker: cleaned up code.
