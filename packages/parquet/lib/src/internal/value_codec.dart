import 'dart:convert';
import 'dart:typed_data';

import '../error.dart';
import '../schema.dart';
import 'binary_reader.dart';
import 'binary_writer.dart';

Uint8List encodePlainValues(ParquetPrimitiveType type, List<Object?> values) {
  final writer = BinaryWriter();
  switch (type.physicalType) {
    case ParquetPhysicalType.boolean:
      _encodeBooleans(writer, values.cast<bool>());
    case ParquetPhysicalType.int32:
      for (final value in values.cast<int>()) {
        writer.writeInt32LE(value);
      }
    case ParquetPhysicalType.int64:
      for (final value in values.cast<int>()) {
        writer.writeInt64LE(value);
      }
    case ParquetPhysicalType.float:
      for (final value in values.cast<num>()) {
        writer.writeFloat32LE(value.toDouble());
      }
    case ParquetPhysicalType.doubleType:
      for (final value in values.cast<num>()) {
        writer.writeFloat64LE(value.toDouble());
      }
    case ParquetPhysicalType.byteArray:
      for (final value in values) {
        final bytes = switch (value) {
          String string => Uint8List.fromList(utf8.encode(string)),
          Uint8List bytes => bytes,
          List<int> bytes => Uint8List.fromList(bytes),
          _ => throw ParquetValidationException(
            'Expected String or bytes for BYTE_ARRAY field',
          ),
        };
        writer.writeInt32LE(bytes.length);
        writer.writeBytes(bytes);
      }
    case ParquetPhysicalType.int96:
    case ParquetPhysicalType.fixedLenByteArray:
      throw ParquetUnsupportedError(
        'Encoding ${type.physicalType.name} values is not supported in v1',
      );
  }
  return writer.takeBytes();
}

List<Object?> decodePlainValues(
  ParquetPrimitiveType type,
  Uint8List bytes,
  int valueCount,
) {
  final reader = BinaryReader(bytes);
  return switch (type.physicalType) {
    ParquetPhysicalType.boolean => _decodeBooleans(bytes, valueCount),
    ParquetPhysicalType.int32 => List<Object?>.generate(
      valueCount,
      (_) => reader.readInt32LE(),
    ),
    ParquetPhysicalType.int64 => List<Object?>.generate(
      valueCount,
      (_) => reader.readInt64LE(),
    ),
    ParquetPhysicalType.float => List<Object?>.generate(
      valueCount,
      (_) => reader.readFloat32LE(),
    ),
    ParquetPhysicalType.doubleType => List<Object?>.generate(
      valueCount,
      (_) => reader.readFloat64LE(),
    ),
    ParquetPhysicalType.byteArray => List<Object?>.generate(valueCount, (_) {
      final length = reader.readInt32LE();
      final value = reader.readBytes(length);
      if (type.logicalType is ParquetStringType) {
        return utf8.decode(value);
      }
      return value;
    }),
    ParquetPhysicalType.int96 ||
    ParquetPhysicalType.fixedLenByteArray => throw ParquetUnsupportedError(
      'Decoding ${type.physicalType.name} values is not supported in v1',
    ),
  };
}

void validateValueForType(ParquetField field, Object? value) {
  if (value == null) {
    if (field.repetition == ParquetRepetition.required) {
      throw ParquetValidationException(
        'Missing required field `${field.name}`',
      );
    }
    return;
  }

  if (field.type case final ParquetPrimitiveType primitive) {
    switch (primitive.physicalType) {
      case ParquetPhysicalType.boolean:
        if (value is! bool) {
          throw ParquetValidationException(
            'Field `${field.name}` expects a bool',
          );
        }
      case ParquetPhysicalType.int32:
      case ParquetPhysicalType.int64:
        if (value is! int) {
          throw ParquetValidationException(
            'Field `${field.name}` expects an int',
          );
        }
      case ParquetPhysicalType.float:
      case ParquetPhysicalType.doubleType:
        if (value is! num) {
          throw ParquetValidationException(
            'Field `${field.name}` expects a num',
          );
        }
      case ParquetPhysicalType.byteArray:
        if (value is! String && value is! Uint8List && value is! List<int>) {
          throw ParquetValidationException(
            'Field `${field.name}` expects a String or byte list',
          );
        }
      case ParquetPhysicalType.int96:
      case ParquetPhysicalType.fixedLenByteArray:
        throw ParquetUnsupportedError(
          'Field `${field.name}` uses unsupported physical type `${primitive.physicalType.name}`',
        );
    }
    return;
  }

  throw ParquetUnsupportedError(
    'Nested and repeated fields are not supported for v1 row read/write',
  );
}

void _encodeBooleans(BinaryWriter writer, List<bool> values) {
  var currentByte = 0;
  var bitOffset = 0;
  for (final value in values) {
    if (value) {
      currentByte |= 1 << bitOffset;
    }
    bitOffset++;
    if (bitOffset == 8) {
      writer.writeByte(currentByte);
      currentByte = 0;
      bitOffset = 0;
    }
  }
  if (bitOffset > 0) {
    writer.writeByte(currentByte);
  }
}

List<Object?> _decodeBooleans(Uint8List bytes, int valueCount) {
  final values = <Object?>[];
  for (final byte in bytes) {
    for (var bit = 0; bit < 8 && values.length < valueCount; bit++) {
      values.add(((byte >> bit) & 1) == 1);
    }
  }
  return values;
}
