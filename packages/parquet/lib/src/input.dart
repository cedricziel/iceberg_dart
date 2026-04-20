import 'dart:io';
import 'dart:typed_data';

abstract interface class ParquetInput {
  Future<int> get length;

  Future<Uint8List> readRange(int start, int end);
}

final class FileParquetInput implements ParquetInput {
  FileParquetInput(this.file);

  final File file;

  @override
  Future<int> get length => file.length();

  @override
  Future<Uint8List> readRange(int start, int end) async {
    final randomAccessFile = await file.open();
    try {
      await randomAccessFile.setPosition(start);
      return Uint8List.fromList(await randomAccessFile.read(end - start));
    } finally {
      await randomAccessFile.close();
    }
  }
}

final class BytesParquetInput implements ParquetInput {
  BytesParquetInput(Uint8List bytes) : _bytes = Uint8List.fromList(bytes);

  final Uint8List _bytes;

  @override
  Future<int> get length async => _bytes.length;

  @override
  Future<Uint8List> readRange(int start, int end) async {
    return Uint8List.sublistView(_bytes, start, end);
  }
}
