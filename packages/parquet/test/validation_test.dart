import 'package:parquet/parquet.dart';
import 'package:test/test.dart';

void main() {
  group('validation and unsupported features', () {
    test('rejects missing required fields', () async {
      final schema = ParquetSchema([
        const ParquetField.required(
          name: 'id',
          type: ParquetPrimitiveType(physicalType: ParquetPhysicalType.int32),
        ),
      ]);

      expect(
        () => ParquetOutput.writeToBytes(const [
          <String, Object?>{},
        ], schema: schema),
        throwsA(isA<ParquetValidationException>()),
      );
    });

    test('rejects unsupported compression', () async {
      final schema = ParquetSchema([
        const ParquetField.required(
          name: 'id',
          type: ParquetPrimitiveType(physicalType: ParquetPhysicalType.int32),
        ),
      ]);

      expect(
        () => ParquetOutput.writeToBytes(
          const [
            <String, Object?>{'id': 1},
          ],
          schema: schema,
          options: const ParquetWriteOptions(
            compression: ParquetCompression.snappy,
          ),
        ),
        throwsA(isA<ParquetUnsupportedError>()),
      );
    });

    test('rejects repeated fields for v1 writes', () async {
      final schema = ParquetSchema([
        const ParquetField.repeated(
          name: 'ids',
          type: ParquetPrimitiveType(physicalType: ParquetPhysicalType.int32),
        ),
      ]);

      expect(
        () => ParquetOutput.writeToBytes(const [
          <String, Object?>{'ids': 1},
        ], schema: schema),
        throwsA(isA<ParquetUnsupportedError>()),
      );
    });
  });
}
