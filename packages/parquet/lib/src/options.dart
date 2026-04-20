import 'metadata.dart';

final class ParquetReadOptions {
  const ParquetReadOptions({this.columns, this.validateLogicalTypes = true});

  final List<String>? columns;
  final bool validateLogicalTypes;
}

final class ParquetWriteOptions {
  const ParquetWriteOptions({
    this.compression = ParquetCompression.uncompressed,
    this.rowGroupSize,
    this.validateRows = true,
  });

  final ParquetCompression compression;
  final int? rowGroupSize;
  final bool validateRows;
}
