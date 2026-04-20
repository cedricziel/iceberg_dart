import 'dart:typed_data';

import 'package:parquet/parquet.dart';
import 'package:test/test.dart';

void main() {
  group('malformed files', () {
    test('rejects invalid magic bytes', () async {
      final bytes = Uint8List.fromList('NOTP'.codeUnits);
      expect(
        () => Parquet.readMetadataFromBytes(bytes),
        throwsA(isA<ParquetFormatException>()),
      );
    });

    test('rejects truncated footer', () async {
      final bytes = Uint8List.fromList([
        ...'PAR1'.codeUnits,
        1,
        2,
        3,
        4,
        ...'PAR1'.codeUnits,
      ]);
      expect(
        () => Parquet.readMetadataFromBytes(bytes),
        throwsA(isA<ParquetFormatException>()),
      );
    });
  });
}
