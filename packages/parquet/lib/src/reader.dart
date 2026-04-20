import 'metadata.dart';
import 'schema.dart';

abstract interface class ParquetReader {
  ParquetSchema get schema;
  ParquetFileMetadata get metadata;

  Stream<ParquetRow> readRows();

  Future<void> close();
}
