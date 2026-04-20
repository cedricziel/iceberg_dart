# iceberg_dart

A Dart workspace for Parquet experimentation and related data tooling.

## Structure

- `packages/iceberg_dart`: CLI package scaffolded with `dart create`
- `packages/parquet`: schema-first Parquet library with async file, bytes, and sink APIs

## Commands

```sh
dart pub get
dart run packages/iceberg_dart/bin/iceberg_dart.dart
dart test packages/iceberg_dart/test
dart test packages/parquet/test
dart analyze
```
