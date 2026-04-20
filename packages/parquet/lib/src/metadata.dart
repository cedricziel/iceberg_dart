import 'schema.dart';

enum ParquetCompression {
  uncompressed,
  snappy,
  gzip,
  lzo,
  brotli,
  lz4,
  zstd,
  lz4Raw,
}

final class ParquetColumnChunkMetadata {
  const ParquetColumnChunkMetadata({
    required this.path,
    required this.physicalType,
    this.logicalType,
    required this.compression,
    required this.valueCount,
    required this.totalCompressedSize,
    required this.totalUncompressedSize,
    this.fileOffset,
    this.dataPageOffset,
    this.dictionaryPageOffset,
  });

  final List<String> path;
  final ParquetPhysicalType physicalType;
  final ParquetLogicalType? logicalType;
  final ParquetCompression compression;
  final int valueCount;
  final int totalCompressedSize;
  final int totalUncompressedSize;
  final int? fileOffset;
  final int? dataPageOffset;
  final int? dictionaryPageOffset;
}

final class ParquetRowGroupMetadata {
  const ParquetRowGroupMetadata({
    required this.rowCount,
    required this.totalByteSize,
    required this.columns,
  });

  final int rowCount;
  final int totalByteSize;
  final List<ParquetColumnChunkMetadata> columns;
}

final class ParquetFileMetadata {
  const ParquetFileMetadata({
    required this.version,
    required this.schema,
    required this.rowCount,
    required this.rowGroups,
    this.createdBy,
    this.keyValueMetadata = const <String, String>{},
  });

  final int version;
  final ParquetSchema schema;
  final int rowCount;
  final List<ParquetRowGroupMetadata> rowGroups;
  final String? createdBy;
  final Map<String, String> keyValueMetadata;
}
