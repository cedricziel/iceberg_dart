import 'schema.dart';

abstract interface class ParquetWriter {
  ParquetSchema get schema;

  Future<void> writeRow(ParquetRow row);

  Future<void> writeRows(Iterable<ParquetRow> rows);

  Future<void> close();
}
