import 'dart:typed_data';

import '../error.dart';
import 'binary_reader.dart';
import 'binary_writer.dart';

Uint8List encodeDefinitionLevels(List<int> levels, {required int bitWidth}) {
  final writer = BinaryWriter();
  if (bitWidth == 0) {
    writer.writeInt32LE(0);
    return writer.takeBytes();
  }

  final payload = BinaryWriter();
  var index = 0;
  while (index < levels.length) {
    final value = levels[index];
    var runLength = 1;
    while (index + runLength < levels.length &&
        levels[index + runLength] == value) {
      runLength++;
    }
    payload.writeByte(runLength << 1);
    payload.writeByte(value);
    index += runLength;
  }

  final payloadBytes = payload.takeBytes();
  writer.writeInt32LE(payloadBytes.length);
  writer.writeBytes(payloadBytes);
  return writer.takeBytes();
}

List<int> decodeDefinitionLevels(
  Uint8List bytes, {
  required int bitWidth,
  required int valueCount,
}) {
  if (bitWidth == 0) {
    return List<int>.filled(valueCount, 0);
  }
  final reader = BinaryReader(bytes);
  final payloadLength = reader.readInt32LE();
  final payload = BinaryReader(reader.readBytes(payloadLength));
  final levels = <int>[];
  while (!payload.isEOF && levels.length < valueCount) {
    final header = payload.readByte();
    if ((header & 1) == 0) {
      final runLength = header >> 1;
      final value = payload.readByte();
      levels.addAll(List<int>.filled(runLength, value));
    } else {
      final groups = header >> 1;
      final count = groups * 8;
      final byteCount = (count * bitWidth + 7) ~/ 8;
      final packed = payload.readBytes(byteCount);
      levels.addAll(_decodeBitPacked(packed, bitWidth, count));
    }
  }
  if (levels.length < valueCount) {
    throw const ParquetFormatException('Insufficient definition levels');
  }
  return levels.take(valueCount).toList();
}

List<int> _decodeBitPacked(Uint8List bytes, int bitWidth, int valueCount) {
  final values = <int>[];
  var currentByte = 0;
  var bitsRemaining = 0;
  var offset = 0;
  for (var index = 0; index < valueCount; index++) {
    var value = 0;
    var written = 0;
    while (written < bitWidth) {
      if (bitsRemaining == 0) {
        currentByte = bytes[offset++];
        bitsRemaining = 8;
      }
      final take = (bitWidth - written) < bitsRemaining
          ? (bitWidth - written)
          : bitsRemaining;
      final mask = (1 << take) - 1;
      value |= (currentByte & mask) << written;
      currentByte >>= take;
      bitsRemaining -= take;
      written += take;
    }
    values.add(value);
  }
  return values;
}
