import 'package:parquet/parquet.dart';
import 'package:test/test.dart';

void main() {
  group('round trip', () {
    test('writes and reads flat primitive rows', () async {
      final schema = ParquetSchema([
        const ParquetField.required(
          name: 'id',
          type: ParquetPrimitiveType(physicalType: ParquetPhysicalType.int64),
        ),
        const ParquetField.optional(
          name: 'name',
          type: ParquetPrimitiveType(
            physicalType: ParquetPhysicalType.byteArray,
            logicalType: ParquetStringType(),
          ),
        ),
        const ParquetField.optional(
          name: 'score',
          type: ParquetPrimitiveType(
            physicalType: ParquetPhysicalType.doubleType,
          ),
        ),
      ]);

      final bytes = await ParquetOutput.writeToBytes([
        const <String, Object?>{'id': 1, 'name': 'Ada', 'score': 9.5},
        const <String, Object?>{'id': 2, 'name': null, 'score': 7.0},
      ], schema: schema);

      final metadata = await Parquet.readMetadataFromBytes(bytes);
      expect(metadata.version, 1);
      expect(metadata.rowCount, 2);
      expect(metadata.rowGroups, hasLength(1));
      expect(metadata.rowGroups.single.columns, hasLength(3));

      final reader = await Parquet.openBytes(bytes);
      final rows = await reader.readRows().toList();
      await reader.close();

      expect(rows, hasLength(2));
      expect(rows[0], {'id': 1, 'name': 'Ada', 'score': 9.5});
      expect(rows[1], {'id': 2, 'name': null, 'score': 7.0});
    });
  });
}
