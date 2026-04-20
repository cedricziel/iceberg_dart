import 'package:parquet/parquet.dart';

Future<void> main() async {
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

  final bytes = await ParquetOutput.writeToBytes([
    {'id': 1, 'name': 'Ada'},
    {'id': 2, 'name': null},
  ], schema: schema);

  final reader = await Parquet.openBytes(bytes);
  await for (final row in reader.readRows()) {
    print(row);
  }
  await reader.close();
}
