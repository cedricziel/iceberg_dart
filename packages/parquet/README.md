# parquet

Schema-first Parquet reading and writing for Dart.

This package exposes public types for schema composition, metadata inspection, async file and byte reads, and schema-driven writing. The first implementation intentionally targets a small, explicit v1 subset rather than claiming broad Parquet support.

## V1 Coverage Matrix

### Read support

| Feature | Status | Notes |
| --- | --- | --- |
| File path and `File` inputs | Supported | Async APIs via `Parquet.openFile` |
| In-memory bytes | Supported | Async APIs via `Parquet.openBytes` |
| Footer metadata decoding | Supported | Internal Compact Protocol implementation |
| Flat primitive schemas | Supported | Nested and repeated row reads are out of scope for v1 |
| `BOOLEAN`, `INT32`, `INT64`, `FLOAT`, `DOUBLE`, `BYTE_ARRAY` | Supported | `BYTE_ARRAY` maps to `String` when annotated as string |
| Required and optional fields | Supported | Optional fields use definition levels |
| Data Page V1 | Supported | Uncompressed only |
| `PLAIN` value encoding | Supported | Used for primitive values |
| `RLE` definition levels | Supported | Required for optional fields |
| Compression codecs other than `UNCOMPRESSED` | Unsupported | Explicit unsupported-feature errors |
| Dictionary pages and dictionary encodings | Unsupported | Explicit unsupported-feature errors |
| Data Page V2 | Unsupported | Explicit unsupported-feature errors |
| Nested and repeated row extraction | Unsupported | Public types exist, row reads do not yet support them |
| `INT96`, `FIXED_LEN_BYTE_ARRAY` values | Unsupported | Metadata may decode, row value decoding does not |

### Write support

| Feature | Status | Notes |
| --- | --- | --- |
| File outputs | Supported | Async APIs via `ParquetOutput.openFile` |
| Sink-style outputs | Supported | Async APIs via `ParquetOutput.openSink` |
| In-memory bytes | Supported | Async APIs via `ParquetOutput.writeToBytes` |
| Flat primitive schemas | Supported | Schema must be declared explicitly |
| `BOOLEAN`, `INT32`, `INT64`, `FLOAT`, `DOUBLE`, `BYTE_ARRAY` | Supported | `BYTE_ARRAY` accepts `String` or bytes |
| Required and optional fields | Supported | Optional fields use definition levels |
| Data Page V1 | Supported | Uncompressed only |
| `PLAIN` value encoding | Supported | Used for primitive values |
| Compression codecs other than `UNCOMPRESSED` | Unsupported | Explicit unsupported-feature errors |
| Dictionary pages and dictionary encodings | Unsupported | Explicit unsupported-feature errors |
| Nested and repeated schemas | Unsupported | Public schema types exist, writer rejects them in v1 |

## Supported Dart Type Mappings

| Parquet physical/logical type | Dart value |
| --- | --- |
| `BOOLEAN` | `bool` |
| `INT32` | `int` |
| `INT64` | `int` |
| `FLOAT` | `num` |
| `DOUBLE` | `num` |
| `BYTE_ARRAY` | `String`, `Uint8List`, or `List<int>` |

## Usage

```dart
import 'package:parquet/parquet.dart';

final schema = ParquetSchema([
  const ParquetField.required(
    name: 'id',
    type: ParquetPrimitiveType(physicalType: ParquetPhysicalType.int64),
  ),
  const ParquetField.optional(
    name: 'name',
    type: ParquetPrimitiveType(
      physicalType: ParquetPhysicalType.byteArray,
      logicalType: ParquetStringType(),
    ),
  ),
]);
```

```dart
import 'package:parquet/parquet.dart';

final bytes = await ParquetOutput.writeToBytes(
  [
    {'id': 1, 'name': 'Ada'},
    {'id': 2, 'name': null},
  ],
  schema: schema,
);

final reader = await Parquet.openBytes(bytes);
await for (final row in reader.readRows()) {
  print(row);
}
await reader.close();
```

## Test Fixtures

The test suite uses:

- curated upstream fixtures from `apache/parquet-testing`
- small local fixtures generated through the package API for round-trip and contract tests
