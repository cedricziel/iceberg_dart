import 'dart:typed_data';

import '../error.dart';
import 'binary_reader.dart';
import 'binary_writer.dart';

enum ThriftType {
  stop(0),
  trueValue(1),
  falseValue(2),
  byte(3),
  i16(4),
  i32(5),
  i64(6),
  doubleValue(7),
  binary(8),
  list(9),
  set(10),
  map(11),
  struct(12);

  const ThriftType(this.wireValue);

  final int wireValue;

  static ThriftType fromWireValue(int wireValue) {
    for (final value in values) {
      if (value.wireValue == wireValue) {
        return value;
      }
    }
    throw ParquetFormatException('Unsupported thrift wire type: $wireValue');
  }
}

final class ThriftFieldHeader {
  const ThriftFieldHeader({required this.id, required this.type});

  final int id;
  final ThriftType type;
}

final class ThriftListHeader {
  const ThriftListHeader({required this.size, required this.type});

  final int size;
  final ThriftType type;
}

final class ThriftMapHeader {
  const ThriftMapHeader({
    required this.size,
    required this.keyType,
    required this.valueType,
  });

  final int size;
  final ThriftType keyType;
  final ThriftType valueType;
}

final class ThriftCompactReader {
  ThriftCompactReader(Uint8List data) : _reader = BinaryReader(data);

  final BinaryReader _reader;

  int get remaining => _reader.remaining;

  ThriftFieldHeader readFieldHeader(int previousFieldId) {
    final header = _reader.readByte();
    final type = ThriftType.fromWireValue(header & 0x0f);
    if (type == ThriftType.stop) {
      return const ThriftFieldHeader(id: 0, type: ThriftType.stop);
    }
    final fieldDelta = header >> 4;
    final fieldId = fieldDelta == 0 ? readI16() : previousFieldId + fieldDelta;
    return ThriftFieldHeader(id: fieldId, type: type);
  }

  bool readBool(ThriftType type) {
    if (type == ThriftType.trueValue) {
      return true;
    }
    if (type == ThriftType.falseValue) {
      return false;
    }
    return _reader.readByte() != 0;
  }

  int readByte() {
    final value = _reader.readByte();
    return value > 127 ? value - 256 : value;
  }

  int readI16() => _decodeZigZag(_readVarInt());

  int readI32() => _decodeZigZag(_readVarInt());

  int readI64() => _decodeZigZag(_readVarInt());

  double readDouble() {
    final bytes = _reader.readBytes(8);
    return ByteData.sublistView(bytes).getFloat64(0, Endian.little);
  }

  Uint8List readBinary() => _reader.readBytes(_readVarInt());

  String readString() => _reader.readUtf8(_readVarInt());

  ThriftListHeader readListHeader() {
    final header = _reader.readByte();
    var size = header >> 4;
    final type = ThriftType.fromWireValue(header & 0x0f);
    if (size == 15) {
      size = _readVarInt();
    }
    return ThriftListHeader(size: size, type: type);
  }

  ThriftMapHeader readMapHeader() {
    final size = _readVarInt();
    if (size == 0) {
      return const ThriftMapHeader(
        size: 0,
        keyType: ThriftType.stop,
        valueType: ThriftType.stop,
      );
    }
    final keyAndValue = _reader.readByte();
    return ThriftMapHeader(
      size: size,
      keyType: ThriftType.fromWireValue(keyAndValue >> 4),
      valueType: ThriftType.fromWireValue(keyAndValue & 0x0f),
    );
  }

  void skip(ThriftType type) {
    switch (type) {
      case ThriftType.stop:
      case ThriftType.trueValue:
      case ThriftType.falseValue:
        return;
      case ThriftType.byte:
        readByte();
      case ThriftType.i16:
        readI16();
      case ThriftType.i32:
        readI32();
      case ThriftType.i64:
        readI64();
      case ThriftType.doubleValue:
        readDouble();
      case ThriftType.binary:
        readBinary();
      case ThriftType.list:
      case ThriftType.set:
        final list = readListHeader();
        for (var index = 0; index < list.size; index++) {
          skip(list.type);
        }
      case ThriftType.map:
        final map = readMapHeader();
        for (var index = 0; index < map.size; index++) {
          skip(map.keyType);
          skip(map.valueType);
        }
      case ThriftType.struct:
        var previousFieldId = 0;
        while (true) {
          final field = readFieldHeader(previousFieldId);
          if (field.type == ThriftType.stop) {
            return;
          }
          skip(field.type);
          previousFieldId = field.id;
        }
    }
  }

  int _readVarInt() {
    var shift = 0;
    var result = 0;
    while (true) {
      final byte = _reader.readByte();
      result |= (byte & 0x7f) << shift;
      if ((byte & 0x80) == 0) {
        return result;
      }
      shift += 7;
      if (shift > 63) {
        throw const ParquetFormatException('Varint is too large');
      }
    }
  }

  int _decodeZigZag(int value) => (value >> 1) ^ -(value & 1);
}

final class ThriftCompactWriter {
  final BinaryWriter _writer = BinaryWriter();

  int writeFieldHeader({
    required ThriftType type,
    required int fieldId,
    required int previousFieldId,
  }) {
    final delta = fieldId - previousFieldId;
    if (delta > 0 && delta <= 15) {
      _writer.writeByte((delta << 4) | type.wireValue);
    } else {
      _writer.writeByte(type.wireValue);
      writeI16(fieldId);
    }
    return fieldId;
  }

  void writeStructEnd() => _writer.writeByte(0);

  void writeBoolField({
    required bool value,
    required int fieldId,
    required int previousFieldId,
  }) {
    writeFieldHeader(
      type: value ? ThriftType.trueValue : ThriftType.falseValue,
      fieldId: fieldId,
      previousFieldId: previousFieldId,
    );
  }

  void writeByte(int value) => _writer.writeByte(value & 0xff);

  void writeI16(int value) => _writeVarInt(_encodeZigZag(value));

  void writeI32(int value) => _writeVarInt(_encodeZigZag(value));

  void writeI64(int value) => _writeVarInt(_encodeZigZag(value));

  void writeDouble(double value) {
    final data = ByteData(8)..setFloat64(0, value, Endian.little);
    _writer.writeBytes(data.buffer.asUint8List());
  }

  void writeBinary(List<int> value) {
    _writeVarInt(value.length);
    _writer.writeBytes(value);
  }

  void writeString(String value) {
    final bytes = Uint8List.fromList(value.codeUnits);
    _writeVarInt(bytes.length);
    _writer.writeBytes(bytes);
  }

  void writeListHeader(ThriftType type, int size) {
    if (size < 15) {
      _writer.writeByte((size << 4) | type.wireValue);
    } else {
      _writer.writeByte((15 << 4) | type.wireValue);
      _writeVarInt(size);
    }
  }

  Uint8List takeBytes() => _writer.takeBytes();

  int _encodeZigZag(int value) => (value << 1) ^ (value >> 63);

  void _writeVarInt(int value) {
    var current = value;
    while (true) {
      if ((current & ~0x7f) == 0) {
        _writer.writeByte(current);
        return;
      }
      _writer.writeByte((current & 0x7f) | 0x80);
      current >>= 7;
    }
  }
}
