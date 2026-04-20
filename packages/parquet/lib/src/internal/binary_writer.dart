import 'dart:convert';
import 'dart:typed_data';

final class BinaryWriter {
  final BytesBuilder _builder = BytesBuilder(copy: false);

  void writeByte(int value) => _builder.addByte(value & 0xff);

  void writeBytes(List<int> bytes) => _builder.add(bytes);

  void writeInt32LE(int value) {
    final data = ByteData(4)..setInt32(0, value, Endian.little);
    _builder.add(data.buffer.asUint8List());
  }

  void writeInt64LE(int value) {
    final data = ByteData(8)..setInt64(0, value, Endian.little);
    _builder.add(data.buffer.asUint8List());
  }

  void writeFloat32LE(double value) {
    final data = ByteData(4)..setFloat32(0, value, Endian.little);
    _builder.add(data.buffer.asUint8List());
  }

  void writeFloat64LE(double value) {
    final data = ByteData(8)..setFloat64(0, value, Endian.little);
    _builder.add(data.buffer.asUint8List());
  }

  void writeUtf8(String value) => _builder.add(utf8.encode(value));

  Uint8List takeBytes() => _builder.takeBytes();
}
