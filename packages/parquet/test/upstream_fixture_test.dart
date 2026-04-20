import 'dart:io';

import 'package:parquet/parquet.dart';
import 'package:test/test.dart';

void main() {
  final validFixture = File(
    'packages/parquet/test/fixtures/upstream/valid/apache-parquet-testing-binary.parquet',
  );
  final invalidFixture = File(
    'packages/parquet/test/fixtures/upstream/invalid/apache-parquet-testing-PARQUET-1481.parquet',
  );

  group('upstream fixtures', () {
    test('decodes metadata from a valid apache/parquet-testing file', () async {
      final metadata = await Parquet.readMetadataFromFile(validFixture.path);

      expect(metadata.schema.fields, hasLength(1));
      final field = metadata.schema.fields.single;
      expect(field.name, 'foo');
      expect(field.type, isA<ParquetPrimitiveType>());
      final primitive = field.type as ParquetPrimitiveType;
      expect(primitive.physicalType, ParquetPhysicalType.byteArray);
      expect(metadata.rowGroups, hasLength(1));
      expect(metadata.rowCount, greaterThan(0));
    });

    test('rejects malformed apache/parquet-testing metadata', () async {
      expect(
        () => Parquet.readMetadataFromFile(invalidFixture.path),
        throwsA(isA<ParquetFormatException>()),
      );
    });

    test('reads and writes through file-based APIs', () async {
      final directory = await Directory.systemTemp.createTemp('parquet_test');
      addTearDown(() => directory.delete(recursive: true));
      final path = '${directory.path}/sample.parquet';
      final schema = ParquetSchema([
        const ParquetField.required(
          name: 'id',
          type: ParquetPrimitiveType(physicalType: ParquetPhysicalType.int32),
        ),
      ]);

      final writer = await ParquetOutput.openFile(path, schema: schema);
      await writer.writeRow(const {'id': 1});
      await writer.writeRow(const {'id': 2});
      await writer.close();

      final reader = await Parquet.openFile(path);
      final rows = await reader.readRows().toList();
      await reader.close();

      expect(rows, [
        {'id': 1},
        {'id': 2},
      ]);
    });
  });
}
