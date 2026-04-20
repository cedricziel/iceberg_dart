import 'dart:convert';
import 'dart:typed_data';

import '../error.dart';

final class BinaryReader {
  BinaryReader(Uint8List data)
    : _data = data,
      _view = ByteData.sublistView(data);

  final Uint8List _data;
  final ByteData _view;
  int offset = 0;

  bool get isEOF => offset >= _data.length;
  int get remaining => _data.length - offset;

  int readByte() {
    _ensure(1);
    return _data[offset++];
  }

  Uint8List readBytes(int length) {
    _ensure(length);
    final start = offset;
    offset += length;
    return Uint8List.sublistView(_data, start, start + length);
  }

  int readInt32LE() {
    _ensure(4);
    final value = _view.getInt32(offset, Endian.little);
    offset += 4;
    return value;
  }

  int readInt64LE() {
    _ensure(8);
    final value = _view.getInt64(offset, Endian.little);
    offset += 8;
    return value;
  }

  double readFloat32LE() {
    _ensure(4);
    final value = _view.getFloat32(offset, Endian.little);
    offset += 4;
    return value;
  }

  double readFloat64LE() {
    _ensure(8);
    final value = _view.getFloat64(offset, Endian.little);
    offset += 8;
    return value;
  }

  String readUtf8(int length) => utf8.decode(readBytes(length));

  void _ensure(int length) {
    if (remaining < length) {
      throw const ParquetFormatException('Unexpected end of buffer');
    }
  }
}
