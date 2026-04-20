import 'dart:io';
import 'dart:typed_data';

import 'input.dart';
import 'internal/metadata_reader.dart';
import 'internal/reader_impl.dart';
import 'metadata.dart';
import 'options.dart';
import 'reader.dart';

final class Parquet {
  const Parquet._();

  static Future<ParquetReader> openFile(
    String path, {
    ParquetReadOptions options = const ParquetReadOptions(),
  }) async {
    final input = FileParquetInput(File(path));
    return ParquetReaderImpl.open(input, options: options);
  }

  static Future<ParquetReader> openBytes(
    Uint8List bytes, {
    ParquetReadOptions options = const ParquetReadOptions(),
  }) async {
    final input = BytesParquetInput(bytes);
    return ParquetReaderImpl.open(input, options: options);
  }

  static Future<ParquetFileMetadata> readMetadataFromFile(String path) async {
    return readParquetMetadata(FileParquetInput(File(path)));
  }

  static Future<ParquetFileMetadata> readMetadataFromBytes(
    Uint8List bytes,
  ) async {
    return readParquetMetadata(BytesParquetInput(bytes));
  }
}
