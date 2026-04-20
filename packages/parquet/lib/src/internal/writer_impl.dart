import 'dart:async';
import 'dart:typed_data';

import '../error.dart';
import '../metadata.dart';
import '../options.dart';
import '../schema.dart';
import '../writer.dart';
import 'binary_writer.dart';
import 'levels.dart';
import 'thrift_compact.dart';
import 'value_codec.dart';

final class ParquetWriterImpl implements ParquetWriter {
  ParquetWriterImpl._({
    required StreamSink<List<int>> sink,
    required ParquetSchema schema,
    required ParquetWriteOptions options,
  }) : _sink = sink,
       _schema = schema,
       _options = options;

  final StreamSink<List<int>> _sink;
  final ParquetSchema _schema;
  final ParquetWriteOptions _options;
  final List<ParquetRow> _rows = <ParquetRow>[];

  static Future<ParquetWriterImpl> toSink(
    StreamSink<List<int>> sink, {
    required ParquetSchema schema,
    required ParquetWriteOptions options,
  }) async {
    _validateSchema(schema, options);
    return ParquetWriterImpl._(sink: sink, schema: schema, options: options);
  }

  @override
  ParquetSchema get schema => _schema;

  @override
  Future<void> writeRow(ParquetRow row) async {
    if (_options.validateRows) {
      for (final field in _schema.fields) {
        validateValueForType(field, row[field.name]);
      }
    }
    _rows.add(Map<String, Object?>.from(row));
  }

  @override
  Future<void> writeRows(Iterable<ParquetRow> rows) async {
    for (final row in rows) {
      await writeRow(row);
    }
  }

  @override
  Future<void> close() async {
    final writer = BinaryWriter();
    writer.writeUtf8('PAR1');

    final columnChunks = <_WrittenColumnChunk>[];
    for (final field in _schema.fields) {
      columnChunks.add(_writeColumn(field, _rows));
    }

    final rowGroupStart = 4;
    for (final chunk in columnChunks) {
      writer.writeBytes(chunk.bytes);
    }

    final footer = _writeFooter(
      schema: _schema,
      rows: _rows,
      rowGroupStart: rowGroupStart,
      columnChunks: columnChunks,
    );
    writer.writeBytes(footer);
    writer.writeInt32LE(footer.length);
    writer.writeUtf8('PAR1');
    await _sink.addStream(Stream<List<int>>.value(writer.takeBytes()));
    await _sink.close();
  }
}

_WrittenColumnChunk _writeColumn(ParquetField field, List<ParquetRow> rows) {
  final primitive = field.type as ParquetPrimitiveType;
  final definitionLevels = <int>[];
  final values = <Object?>[];
  for (final row in rows) {
    final value = row[field.name];
    if (value == null) {
      definitionLevels.add(0);
    } else {
      definitionLevels.add(
        field.repetition == ParquetRepetition.optional ? 1 : 0,
      );
      values.add(value);
    }
  }

  final levelsBytes = field.repetition == ParquetRepetition.optional
      ? encodeDefinitionLevels(definitionLevels, bitWidth: 1)
      : Uint8List(0);
  final valueBytes = encodePlainValues(primitive, values);
  final pageBody = Uint8List.fromList([...levelsBytes, ...valueBytes]);
  final pageHeader = _writeDataPageHeader(
    numValues: rows.length,
    uncompressedPageSize: pageBody.length,
    compressedPageSize: pageBody.length,
  );

  final bytes = Uint8List.fromList([...pageHeader, ...pageBody]);
  return _WrittenColumnChunk(
    field: field,
    bytes: bytes,
    numValues: rows.length,
    totalCompressedSize: bytes.length,
    totalUncompressedSize: bytes.length,
    pageHeaderLength: pageHeader.length,
    dataPageOffset: 0,
  );
}

Uint8List _writeDataPageHeader({
  required int numValues,
  required int uncompressedPageSize,
  required int compressedPageSize,
}) {
  final writer = ThriftCompactWriter();
  var previousFieldId = 0;
  previousFieldId = writer.writeFieldHeader(
    type: ThriftType.i32,
    fieldId: 1,
    previousFieldId: previousFieldId,
  );
  writer.writeI32(0);
  previousFieldId = writer.writeFieldHeader(
    type: ThriftType.i32,
    fieldId: 2,
    previousFieldId: previousFieldId,
  );
  writer.writeI32(uncompressedPageSize);
  previousFieldId = writer.writeFieldHeader(
    type: ThriftType.i32,
    fieldId: 3,
    previousFieldId: previousFieldId,
  );
  writer.writeI32(compressedPageSize);
  previousFieldId = writer.writeFieldHeader(
    type: ThriftType.struct,
    fieldId: 5,
    previousFieldId: previousFieldId,
  );
  _writeDataPageHeaderStruct(writer, numValues: numValues);
  writer.writeStructEnd();
  return writer.takeBytes();
}

void _writeDataPageHeaderStruct(
  ThriftCompactWriter writer, {
  required int numValues,
}) {
  var previousFieldId = 0;
  previousFieldId = writer.writeFieldHeader(
    type: ThriftType.i32,
    fieldId: 1,
    previousFieldId: previousFieldId,
  );
  writer.writeI32(numValues);
  previousFieldId = writer.writeFieldHeader(
    type: ThriftType.i32,
    fieldId: 2,
    previousFieldId: previousFieldId,
  );
  writer.writeI32(0);
  previousFieldId = writer.writeFieldHeader(
    type: ThriftType.i32,
    fieldId: 3,
    previousFieldId: previousFieldId,
  );
  writer.writeI32(3);
  previousFieldId = writer.writeFieldHeader(
    type: ThriftType.i32,
    fieldId: 4,
    previousFieldId: previousFieldId,
  );
  writer.writeI32(3);
  writer.writeStructEnd();
}

Uint8List _writeFooter({
  required ParquetSchema schema,
  required List<ParquetRow> rows,
  required int rowGroupStart,
  required List<_WrittenColumnChunk> columnChunks,
}) {
  final footer = ThriftCompactWriter();
  var previousFieldId = 0;
  previousFieldId = footer.writeFieldHeader(
    type: ThriftType.i32,
    fieldId: 1,
    previousFieldId: previousFieldId,
  );
  footer.writeI32(1);
  previousFieldId = footer.writeFieldHeader(
    type: ThriftType.list,
    fieldId: 2,
    previousFieldId: previousFieldId,
  );
  _writeSchemaList(footer, schema);
  previousFieldId = footer.writeFieldHeader(
    type: ThriftType.i64,
    fieldId: 3,
    previousFieldId: previousFieldId,
  );
  footer.writeI64(rows.length);
  previousFieldId = footer.writeFieldHeader(
    type: ThriftType.list,
    fieldId: 4,
    previousFieldId: previousFieldId,
  );
  _writeRowGroups(footer, rows.length, rowGroupStart, columnChunks);
  previousFieldId = footer.writeFieldHeader(
    type: ThriftType.binary,
    fieldId: 6,
    previousFieldId: previousFieldId,
  );
  footer.writeString('parquet.dart v1');
  footer.writeStructEnd();
  return footer.takeBytes();
}

void _writeSchemaList(ThriftCompactWriter writer, ParquetSchema schema) {
  final fields = [
    _SchemaNode.root(schema),
    ...schema.fields.map(_SchemaNode.field),
  ];
  writer.writeListHeader(ThriftType.struct, fields.length);
  for (final node in fields) {
    var previousFieldId = 0;
    if (node.physicalType != null) {
      previousFieldId = writer.writeFieldHeader(
        type: ThriftType.i32,
        fieldId: 1,
        previousFieldId: previousFieldId,
      );
      writer.writeI32(_physicalTypeId(node.physicalType!));
    }
    if (node.typeLength != null) {
      previousFieldId = writer.writeFieldHeader(
        type: ThriftType.i32,
        fieldId: 2,
        previousFieldId: previousFieldId,
      );
      writer.writeI32(node.typeLength!);
    }
    if (node.repetition != null) {
      previousFieldId = writer.writeFieldHeader(
        type: ThriftType.i32,
        fieldId: 3,
        previousFieldId: previousFieldId,
      );
      writer.writeI32(_repetitionId(node.repetition!));
    }
    previousFieldId = writer.writeFieldHeader(
      type: ThriftType.binary,
      fieldId: 4,
      previousFieldId: previousFieldId,
    );
    writer.writeString(node.name);
    if (node.numChildren != null) {
      previousFieldId = writer.writeFieldHeader(
        type: ThriftType.i32,
        fieldId: 5,
        previousFieldId: previousFieldId,
      );
      writer.writeI32(node.numChildren!);
    }
    if (node.convertedTypeId != null) {
      previousFieldId = writer.writeFieldHeader(
        type: ThriftType.i32,
        fieldId: 6,
        previousFieldId: previousFieldId,
      );
      writer.writeI32(node.convertedTypeId!);
    }
    writer.writeStructEnd();
  }
}

void _writeRowGroups(
  ThriftCompactWriter writer,
  int rowCount,
  int rowGroupStart,
  List<_WrittenColumnChunk> columnChunks,
) {
  writer.writeListHeader(ThriftType.struct, 1);
  var previousFieldId = 0;
  previousFieldId = writer.writeFieldHeader(
    type: ThriftType.list,
    fieldId: 1,
    previousFieldId: previousFieldId,
  );
  writer.writeListHeader(ThriftType.struct, columnChunks.length);

  var offset = rowGroupStart;
  for (final chunk in columnChunks) {
    chunk.dataPageOffset = offset + chunk.pageHeaderLength;
    var columnPreviousFieldId = 0;
    columnPreviousFieldId = writer.writeFieldHeader(
      type: ThriftType.i64,
      fieldId: 2,
      previousFieldId: columnPreviousFieldId,
    );
    writer.writeI64(offset);
    columnPreviousFieldId = writer.writeFieldHeader(
      type: ThriftType.struct,
      fieldId: 3,
      previousFieldId: columnPreviousFieldId,
    );
    _writeColumnMetaData(writer, chunk);
    writer.writeStructEnd();
    offset += chunk.bytes.length;
  }
  previousFieldId = writer.writeFieldHeader(
    type: ThriftType.i64,
    fieldId: 2,
    previousFieldId: previousFieldId,
  );
  writer.writeI64(
    columnChunks.fold<int>(0, (sum, chunk) => sum + chunk.bytes.length),
  );
  previousFieldId = writer.writeFieldHeader(
    type: ThriftType.i64,
    fieldId: 3,
    previousFieldId: previousFieldId,
  );
  writer.writeI64(rowCount);
  writer.writeStructEnd();
}

void _writeColumnMetaData(
  ThriftCompactWriter writer,
  _WrittenColumnChunk chunk,
) {
  final primitive = chunk.field.type as ParquetPrimitiveType;
  var previousFieldId = 0;
  previousFieldId = writer.writeFieldHeader(
    type: ThriftType.i32,
    fieldId: 1,
    previousFieldId: previousFieldId,
  );
  writer.writeI32(_physicalTypeId(primitive.physicalType));
  previousFieldId = writer.writeFieldHeader(
    type: ThriftType.list,
    fieldId: 2,
    previousFieldId: previousFieldId,
  );
  writer.writeListHeader(ThriftType.i32, 2);
  writer.writeI32(0);
  writer.writeI32(3);
  previousFieldId = writer.writeFieldHeader(
    type: ThriftType.list,
    fieldId: 3,
    previousFieldId: previousFieldId,
  );
  writer.writeListHeader(ThriftType.binary, 1);
  writer.writeString(chunk.field.name);
  previousFieldId = writer.writeFieldHeader(
    type: ThriftType.i32,
    fieldId: 4,
    previousFieldId: previousFieldId,
  );
  writer.writeI32(0);
  previousFieldId = writer.writeFieldHeader(
    type: ThriftType.i64,
    fieldId: 5,
    previousFieldId: previousFieldId,
  );
  writer.writeI64(chunk.numValues);
  previousFieldId = writer.writeFieldHeader(
    type: ThriftType.i64,
    fieldId: 6,
    previousFieldId: previousFieldId,
  );
  writer.writeI64(chunk.totalUncompressedSize);
  previousFieldId = writer.writeFieldHeader(
    type: ThriftType.i64,
    fieldId: 7,
    previousFieldId: previousFieldId,
  );
  writer.writeI64(chunk.totalCompressedSize);
  previousFieldId = writer.writeFieldHeader(
    type: ThriftType.i64,
    fieldId: 9,
    previousFieldId: previousFieldId,
  );
  writer.writeI64(chunk.dataPageOffset!);
  writer.writeStructEnd();
}

void _validateSchema(ParquetSchema schema, ParquetWriteOptions options) {
  if (options.compression != ParquetCompression.uncompressed) {
    throw ParquetUnsupportedError(
      'Compression `${options.compression.name}` is not supported in v1 writes',
    );
  }
  for (final field in schema.fields) {
    if (field.type is! ParquetPrimitiveType ||
        field.repetition == ParquetRepetition.repeated) {
      throw const ParquetUnsupportedError(
        'v1 writes only support flat primitive fields',
      );
    }
    final primitive = field.type as ParquetPrimitiveType;
    if (primitive.physicalType == ParquetPhysicalType.int96 ||
        primitive.physicalType == ParquetPhysicalType.fixedLenByteArray) {
      throw ParquetUnsupportedError(
        'v1 writes do not support `${primitive.physicalType.name}` fields',
      );
    }
  }
}

int _physicalTypeId(ParquetPhysicalType type) => switch (type) {
  ParquetPhysicalType.boolean => 0,
  ParquetPhysicalType.int32 => 1,
  ParquetPhysicalType.int64 => 2,
  ParquetPhysicalType.int96 => 3,
  ParquetPhysicalType.float => 4,
  ParquetPhysicalType.doubleType => 5,
  ParquetPhysicalType.byteArray => 6,
  ParquetPhysicalType.fixedLenByteArray => 7,
};

int _repetitionId(ParquetRepetition repetition) => switch (repetition) {
  ParquetRepetition.required => 0,
  ParquetRepetition.optional => 1,
  ParquetRepetition.repeated => 2,
};

final class _SchemaNode {
  const _SchemaNode({
    required this.name,
    this.repetition,
    this.physicalType,
    this.typeLength,
    this.numChildren,
    this.convertedTypeId,
  });

  factory _SchemaNode.root(ParquetSchema schema) =>
      _SchemaNode(name: schema.name, numChildren: schema.fields.length);

  factory _SchemaNode.field(ParquetField field) {
    final primitive = field.type as ParquetPrimitiveType;
    return _SchemaNode(
      name: field.name,
      repetition: field.repetition,
      physicalType: primitive.physicalType,
      typeLength: primitive.typeLength,
      convertedTypeId: _convertedTypeId(primitive.logicalType),
    );
  }

  final String name;
  final ParquetRepetition? repetition;
  final ParquetPhysicalType? physicalType;
  final int? typeLength;
  final int? numChildren;
  final int? convertedTypeId;
}

int? _convertedTypeId(ParquetLogicalType? logicalType) => switch (logicalType) {
  ParquetStringType() => 0,
  ParquetMapType() => 1,
  ParquetListType() => 3,
  ParquetDecimalType() => 5,
  ParquetDateType() => 6,
  ParquetTimestampType(unit: ParquetTimeUnit.millis) => 9,
  ParquetTimestampType(unit: ParquetTimeUnit.micros) => 10,
  _ => null,
};

final class _WrittenColumnChunk {
  _WrittenColumnChunk({
    required this.field,
    required this.bytes,
    required this.numValues,
    required this.totalCompressedSize,
    required this.totalUncompressedSize,
    required this.pageHeaderLength,
    required this.dataPageOffset,
  });

  final ParquetField field;
  final Uint8List bytes;
  final int numValues;
  final int totalCompressedSize;
  final int totalUncompressedSize;
  final int pageHeaderLength;
  int? dataPageOffset;
}
