import 'dart:typed_data';

import '../error.dart';
import '../input.dart';
import '../metadata.dart';
import '../schema.dart';
import 'thrift_compact.dart';

const _magic = 'PAR1';

Future<ParquetFileMetadata> readParquetMetadata(ParquetInput input) async {
  final length = await input.length;
  if (length < 12) {
    throw const ParquetFormatException('Parquet file is too small');
  }

  final header = await input.readRange(0, 4);
  final footer = await input.readRange(length - 4, length);
  if (String.fromCharCodes(header) != _magic) {
    throw const ParquetFormatException('Missing PAR1 file header');
  }
  if (String.fromCharCodes(footer) != _magic) {
    throw const ParquetFormatException('Missing PAR1 file footer');
  }

  final footerLengthBytes = await input.readRange(length - 8, length - 4);
  final footerLength = ByteData.sublistView(
    footerLengthBytes,
  ).getInt32(0, Endian.little);
  final footerStart = length - 8 - footerLength;
  if (footerStart < 4) {
    throw const ParquetFormatException('Invalid footer length');
  }

  final footerBytes = await input.readRange(footerStart, length - 8);
  return _MetadataParser(footerBytes).parse();
}

final class _MetadataParser {
  _MetadataParser(Uint8List bytes) : _reader = ThriftCompactReader(bytes);

  final ThriftCompactReader _reader;

  ParquetFileMetadata parse() {
    var version = 0;
    var schemaElements = <_SchemaElement>[];
    var rowGroups = <_RowGroup>[];
    var rowCount = 0;
    var createdBy = '';
    final keyValues = <String, String>{};

    var previousFieldId = 0;
    while (true) {
      final field = _reader.readFieldHeader(previousFieldId);
      if (field.type == ThriftType.stop) {
        break;
      }
      switch (field.id) {
        case 1:
          version = _reader.readI32();
        case 2:
          schemaElements = _readSchemaElements();
        case 3:
          rowCount = _reader.readI64();
        case 4:
          rowGroups = _readRowGroups();
        case 5:
          keyValues.addAll(_readKeyValues());
        case 6:
          createdBy = _reader.readString();
        default:
          _reader.skip(field.type);
      }
      previousFieldId = field.id;
    }

    if (schemaElements.isEmpty) {
      throw const ParquetFormatException('Footer does not contain a schema');
    }

    final schema = _buildSchema(schemaElements, rowGroups);
    return ParquetFileMetadata(
      version: version,
      schema: schema,
      rowCount: rowCount,
      rowGroups: rowGroups.map((group) => group.toPublic(schema)).toList(),
      createdBy: createdBy.isEmpty ? null : createdBy,
      keyValueMetadata: keyValues,
    );
  }

  List<_SchemaElement> _readSchemaElements() {
    final header = _reader.readListHeader();
    final values = <_SchemaElement>[];
    for (var index = 0; index < header.size; index++) {
      values.add(_readSchemaElement());
    }
    return values;
  }

  _SchemaElement _readSchemaElement() {
    ParquetPhysicalType? physicalType;
    ParquetRepetition? repetition;
    String? name;
    int? numChildren;
    int? typeLength;
    int? precision;
    int? scale;
    int? fieldId;
    ParquetLogicalType? logicalType;
    _ConvertedType? convertedType;

    var previousFieldId = 0;
    while (true) {
      final field = _reader.readFieldHeader(previousFieldId);
      if (field.type == ThriftType.stop) {
        break;
      }
      switch (field.id) {
        case 1:
          physicalType = _physicalTypeFromId(_reader.readI32());
        case 2:
          typeLength = _reader.readI32();
        case 3:
          repetition = _repetitionFromId(_reader.readI32());
        case 4:
          name = _reader.readString();
        case 5:
          numChildren = _reader.readI32();
        case 6:
          convertedType = _ConvertedType.fromId(_reader.readI32());
        case 7:
          scale = _reader.readI32();
        case 8:
          precision = _reader.readI32();
        case 9:
          fieldId = _reader.readI32();
        case 10:
          logicalType = _readLogicalType();
        default:
          _reader.skip(field.type);
      }
      previousFieldId = field.id;
    }

    return _SchemaElement(
      physicalType: physicalType,
      repetition: repetition,
      name: name ?? 'field',
      numChildren: numChildren,
      typeLength: typeLength,
      precision: precision,
      scale: scale,
      fieldId: fieldId,
      logicalType:
          logicalType ??
          _logicalTypeFromConvertedType(
            convertedType,
            precision: precision,
            scale: scale,
          ),
    );
  }

  ParquetLogicalType? _readLogicalType() {
    var previousFieldId = 0;
    final field = _reader.readFieldHeader(previousFieldId);
    if (field.type == ThriftType.stop) {
      return null;
    }
    previousFieldId = field.id;
    final logicalType = switch (field.id) {
      1 => const ParquetStringType(),
      2 => const ParquetMapType(),
      3 => const ParquetListType(),
      5 => _readDecimalType(),
      6 => const ParquetDateType(),
      8 => _readTimestampType(),
      10 => _readIntType(),
      _ => null,
    };

    if (logicalType == null) {
      _reader.skip(field.type);
    } else if (field.type == ThriftType.struct) {
      // Structured unions are already consumed by dedicated readers.
    }

    final end = _reader.readFieldHeader(previousFieldId);
    if (end.type != ThriftType.stop) {
      throw const ParquetFormatException(
        'LogicalType union has multiple fields',
      );
    }
    return logicalType;
  }

  ParquetLogicalType _readDecimalType() {
    var scale = 0;
    var precision = 0;
    var previousFieldId = 0;
    while (true) {
      final field = _reader.readFieldHeader(previousFieldId);
      if (field.type == ThriftType.stop) {
        break;
      }
      switch (field.id) {
        case 1:
          scale = _reader.readI32();
        case 2:
          precision = _reader.readI32();
        default:
          _reader.skip(field.type);
      }
      previousFieldId = field.id;
    }
    return ParquetDecimalType(precision: precision, scale: scale);
  }

  ParquetLogicalType _readTimestampType() {
    var isAdjustedToUtc = false;
    ParquetTimeUnit unit = ParquetTimeUnit.micros;
    var previousFieldId = 0;
    while (true) {
      final field = _reader.readFieldHeader(previousFieldId);
      if (field.type == ThriftType.stop) {
        break;
      }
      switch (field.id) {
        case 1:
          isAdjustedToUtc = _reader.readBool(field.type);
        case 2:
          unit = _readTimeUnit();
        default:
          _reader.skip(field.type);
      }
      previousFieldId = field.id;
    }
    return ParquetTimestampType(unit: unit, isAdjustedToUtc: isAdjustedToUtc);
  }

  ParquetTimeUnit _readTimeUnit() {
    final field = _reader.readFieldHeader(0);
    if (field.type == ThriftType.stop) {
      throw const ParquetFormatException('TimeUnit union is empty');
    }
    final unit = switch (field.id) {
      1 => ParquetTimeUnit.millis,
      2 => ParquetTimeUnit.micros,
      3 => ParquetTimeUnit.nanos,
      _ => ParquetTimeUnit.micros,
    };
    if (field.id > 3) {
      _reader.skip(field.type);
    } else {
      _reader.skip(field.type);
    }
    final end = _reader.readFieldHeader(field.id);
    if (end.type != ThriftType.stop) {
      throw const ParquetFormatException('TimeUnit union has multiple fields');
    }
    return unit;
  }

  ParquetLogicalType _readIntType() {
    var bitWidth = 32;
    var isSigned = true;
    var previousFieldId = 0;
    while (true) {
      final field = _reader.readFieldHeader(previousFieldId);
      if (field.type == ThriftType.stop) {
        break;
      }
      switch (field.id) {
        case 1:
          bitWidth = _reader.readByte();
        case 2:
          isSigned = _reader.readBool(field.type);
        default:
          _reader.skip(field.type);
      }
      previousFieldId = field.id;
    }
    return ParquetIntType(bitWidth: bitWidth, isSigned: isSigned);
  }

  List<_RowGroup> _readRowGroups() {
    final header = _reader.readListHeader();
    final values = <_RowGroup>[];
    for (var index = 0; index < header.size; index++) {
      values.add(_readRowGroup());
    }
    return values;
  }

  _RowGroup _readRowGroup() {
    var rowCount = 0;
    var totalByteSize = 0;
    var columns = <_ColumnChunk>[];

    var previousFieldId = 0;
    while (true) {
      final field = _reader.readFieldHeader(previousFieldId);
      if (field.type == ThriftType.stop) {
        break;
      }
      switch (field.id) {
        case 1:
          final header = _reader.readListHeader();
          columns = List<_ColumnChunk>.generate(
            header.size,
            (_) => _readColumnChunk(),
          );
        case 2:
          totalByteSize = _reader.readI64();
        case 3:
          rowCount = _reader.readI64();
        default:
          _reader.skip(field.type);
      }
      previousFieldId = field.id;
    }

    return _RowGroup(
      rowCount: rowCount,
      totalByteSize: totalByteSize,
      columns: columns,
    );
  }

  _ColumnChunk _readColumnChunk() {
    var fileOffset = 0;
    _ColumnMetaData? metaData;
    var previousFieldId = 0;
    while (true) {
      final field = _reader.readFieldHeader(previousFieldId);
      if (field.type == ThriftType.stop) {
        break;
      }
      switch (field.id) {
        case 2:
          fileOffset = _reader.readI64();
        case 3:
          metaData = _readColumnMetaData();
        default:
          _reader.skip(field.type);
      }
      previousFieldId = field.id;
    }
    return _ColumnChunk(
      fileOffset: fileOffset,
      metaData:
          metaData ??
          (throw const ParquetFormatException('Column chunk missing metadata')),
    );
  }

  _ColumnMetaData _readColumnMetaData() {
    ParquetPhysicalType? physicalType;
    var compression = ParquetCompression.uncompressed;
    var path = <String>[];
    var valueCount = 0;
    var totalUncompressedSize = 0;
    var totalCompressedSize = 0;
    int? dataPageOffset;
    int? dictionaryPageOffset;
    var previousFieldId = 0;
    while (true) {
      final field = _reader.readFieldHeader(previousFieldId);
      if (field.type == ThriftType.stop) {
        break;
      }
      switch (field.id) {
        case 1:
          physicalType = _physicalTypeFromId(_reader.readI32());
        case 2:
          final list = _reader.readListHeader();
          for (var index = 0; index < list.size; index++) {
            _reader.readI32();
          }
        case 3:
          final list = _reader.readListHeader();
          path = List<String>.generate(list.size, (_) => _reader.readString());
        case 4:
          compression = _compressionFromId(_reader.readI32());
        case 5:
          valueCount = _reader.readI64();
        case 6:
          totalUncompressedSize = _reader.readI64();
        case 7:
          totalCompressedSize = _reader.readI64();
        case 9:
          dataPageOffset = _reader.readI64();
        case 11:
          dictionaryPageOffset = _reader.readI64();
        default:
          _reader.skip(field.type);
      }
      previousFieldId = field.id;
    }

    return _ColumnMetaData(
      physicalType:
          physicalType ??
          (throw const ParquetFormatException('Column metadata missing type')),
      compression: compression,
      path: path,
      valueCount: valueCount,
      totalCompressedSize: totalCompressedSize,
      totalUncompressedSize: totalUncompressedSize,
      dataPageOffset: dataPageOffset,
      dictionaryPageOffset: dictionaryPageOffset,
    );
  }

  Map<String, String> _readKeyValues() {
    final header = _reader.readListHeader();
    final values = <String, String>{};
    for (var index = 0; index < header.size; index++) {
      String? key;
      String? value;
      var previousFieldId = 0;
      while (true) {
        final field = _reader.readFieldHeader(previousFieldId);
        if (field.type == ThriftType.stop) {
          break;
        }
        switch (field.id) {
          case 1:
            key = _reader.readString();
          case 2:
            value = _reader.readString();
          default:
            _reader.skip(field.type);
        }
        previousFieldId = field.id;
      }
      if (key != null) {
        values[key] = value ?? '';
      }
    }
    return values;
  }
}

ParquetSchema _buildSchema(
  List<_SchemaElement> elements,
  List<_RowGroup> rowGroups,
) {
  final root = elements.first;
  final children = <ParquetField>[];
  var index = 1;
  for (var child = 0; child < (root.numChildren ?? 0); child++) {
    final parsed = _buildField(elements, index, rowGroups);
    children.add(parsed.field);
    index = parsed.nextIndex;
  }
  return ParquetSchema(children, name: root.name);
}

_ParsedField _buildField(
  List<_SchemaElement> elements,
  int index,
  List<_RowGroup> rowGroups,
) {
  final element = elements[index];
  if (element.physicalType != null) {
    return _ParsedField(
      field: ParquetField(
        name: element.name,
        repetition: element.repetition ?? ParquetRepetition.optional,
        type: ParquetPrimitiveType(
          physicalType: element.physicalType!,
          logicalType: element.logicalType,
          typeLength: element.typeLength,
          precision: element.precision,
          scale: element.scale,
        ),
        fieldId: element.fieldId,
      ),
      nextIndex: index + 1,
    );
  }

  final children = <ParquetField>[];
  var next = index + 1;
  for (var child = 0; child < (element.numChildren ?? 0); child++) {
    final parsed = _buildField(elements, next, rowGroups);
    children.add(parsed.field);
    next = parsed.nextIndex;
  }
  return _ParsedField(
    field: ParquetField(
      name: element.name,
      repetition: element.repetition ?? ParquetRepetition.optional,
      type: ParquetGroupType(
        fields: children,
        logicalType: element.logicalType,
      ),
      fieldId: element.fieldId,
    ),
    nextIndex: next,
  );
}

ParquetPhysicalType _physicalTypeFromId(int value) => switch (value) {
  0 => ParquetPhysicalType.boolean,
  1 => ParquetPhysicalType.int32,
  2 => ParquetPhysicalType.int64,
  3 => ParquetPhysicalType.int96,
  4 => ParquetPhysicalType.float,
  5 => ParquetPhysicalType.doubleType,
  6 => ParquetPhysicalType.byteArray,
  7 => ParquetPhysicalType.fixedLenByteArray,
  _ => throw ParquetFormatException('Unsupported physical type id: $value'),
};

ParquetRepetition _repetitionFromId(int value) => switch (value) {
  0 => ParquetRepetition.required,
  1 => ParquetRepetition.optional,
  2 => ParquetRepetition.repeated,
  _ => throw ParquetFormatException('Unknown repetition id: $value'),
};

ParquetCompression _compressionFromId(int value) => switch (value) {
  0 => ParquetCompression.uncompressed,
  1 => ParquetCompression.snappy,
  2 => ParquetCompression.gzip,
  3 => ParquetCompression.lzo,
  4 => ParquetCompression.brotli,
  5 => ParquetCompression.lz4,
  6 => ParquetCompression.zstd,
  7 => ParquetCompression.lz4Raw,
  _ => throw ParquetFormatException('Unsupported compression codec id: $value'),
};

ParquetLogicalType? _logicalTypeFromConvertedType(
  _ConvertedType? convertedType, {
  int? precision,
  int? scale,
}) {
  return switch (convertedType) {
    _ConvertedType.utf8 => const ParquetStringType(),
    _ConvertedType.date => const ParquetDateType(),
    _ConvertedType.list => const ParquetListType(),
    _ConvertedType.map => const ParquetMapType(),
    _ConvertedType.decimal when precision != null && scale != null =>
      ParquetDecimalType(precision: precision, scale: scale),
    _ => null,
  };
}

enum _ConvertedType {
  utf8(0),
  map(1),
  list(3),
  decimal(5),
  date(6);

  const _ConvertedType(this.id);

  final int id;

  static _ConvertedType? fromId(int id) {
    for (final value in values) {
      if (value.id == id) {
        return value;
      }
    }
    return null;
  }
}

final class _SchemaElement {
  const _SchemaElement({
    required this.name,
    this.physicalType,
    this.repetition,
    this.numChildren,
    this.typeLength,
    this.precision,
    this.scale,
    this.fieldId,
    this.logicalType,
  });

  final String name;
  final ParquetPhysicalType? physicalType;
  final ParquetRepetition? repetition;
  final int? numChildren;
  final int? typeLength;
  final int? precision;
  final int? scale;
  final int? fieldId;
  final ParquetLogicalType? logicalType;
}

final class _ColumnMetaData {
  const _ColumnMetaData({
    required this.physicalType,
    required this.compression,
    required this.path,
    required this.valueCount,
    required this.totalCompressedSize,
    required this.totalUncompressedSize,
    required this.dataPageOffset,
    required this.dictionaryPageOffset,
  });

  final ParquetPhysicalType physicalType;
  final ParquetCompression compression;
  final List<String> path;
  final int valueCount;
  final int totalCompressedSize;
  final int totalUncompressedSize;
  final int? dataPageOffset;
  final int? dictionaryPageOffset;
}

final class _ColumnChunk {
  const _ColumnChunk({required this.fileOffset, required this.metaData});

  final int fileOffset;
  final _ColumnMetaData metaData;
}

final class _RowGroup {
  const _RowGroup({
    required this.rowCount,
    required this.totalByteSize,
    required this.columns,
  });

  final int rowCount;
  final int totalByteSize;
  final List<_ColumnChunk> columns;

  ParquetRowGroupMetadata toPublic(ParquetSchema schema) {
    final logicalTypesByPath = <String, ParquetLogicalType?>{};
    void collect(ParquetField field, [List<String> prefix = const <String>[]]) {
      final path = [...prefix, field.name];
      if (field.type case final ParquetPrimitiveType primitive) {
        logicalTypesByPath[path.join('.')] = primitive.logicalType;
      } else if (field.type case final ParquetGroupType group) {
        for (final child in group.fields) {
          collect(child, path);
        }
      }
    }

    for (final field in schema.fields) {
      collect(field);
    }

    return ParquetRowGroupMetadata(
      rowCount: rowCount,
      totalByteSize: totalByteSize,
      columns: columns
          .map(
            (column) => ParquetColumnChunkMetadata(
              path: column.metaData.path,
              physicalType: column.metaData.physicalType,
              logicalType: logicalTypesByPath[column.metaData.path.join('.')],
              compression: column.metaData.compression,
              valueCount: column.metaData.valueCount,
              totalCompressedSize: column.metaData.totalCompressedSize,
              totalUncompressedSize: column.metaData.totalUncompressedSize,
              fileOffset: column.fileOffset,
              dataPageOffset: column.metaData.dataPageOffset,
              dictionaryPageOffset: column.metaData.dictionaryPageOffset,
            ),
          )
          .toList(),
    );
  }
}

final class _ParsedField {
  const _ParsedField({required this.field, required this.nextIndex});

  final ParquetField field;
  final int nextIndex;
}
