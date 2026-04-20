import 'dart:async';

import 'package:parquet/parquet.dart';
import 'package:test/test.dart';

void main() {
  group('public schema surface', () {
    test('constructs flat schemas', () {
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
      ]);

      expect(schema.fields, hasLength(2));
      expect(schema.fields.first.repetition, ParquetRepetition.required);
      final nameType = schema.fields.last.type as ParquetPrimitiveType;
      expect(nameType.physicalType, ParquetPhysicalType.byteArray);
      expect(nameType.logicalType, isA<ParquetStringType>());
    });
  });

  group('public writer surface', () {
    test('writes to in-memory bytes asynchronously', () async {
      final schema = ParquetSchema([
        const ParquetField.required(
          name: 'id',
          type: ParquetPrimitiveType(physicalType: ParquetPhysicalType.int32),
        ),
      ]);

      final bytes = await ParquetOutput.writeToBytes(const [
        <String, Object?>{'id': 7},
      ], schema: schema);

      expect(bytes, isNotEmpty);
      final metadata = await Parquet.readMetadataFromBytes(bytes);
      expect(metadata.rowCount, 1);
    });

    test('supports sink-based outputs', () async {
      final schema = ParquetSchema([
        const ParquetField.required(
          name: 'id',
          type: ParquetPrimitiveType(physicalType: ParquetPhysicalType.int64),
        ),
      ]);

      final controller = StreamController<List<int>>();
      final chunks = <int>[];
      final done = controller.stream.listen(chunks.addAll).asFuture<void>();
      final writer = await ParquetOutput.openSink(
        controller.sink,
        schema: schema,
      );
      await writer.writeRow(const {'id': 42});
      await writer.close();
      await controller.close();
      await done;

      expect(chunks, isNotEmpty);
    });
  });
}
