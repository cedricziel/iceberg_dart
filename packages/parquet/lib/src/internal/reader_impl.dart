import 'dart:typed_data';

import '../error.dart';
import '../input.dart';
import '../metadata.dart';
import '../options.dart';
import '../reader.dart';
import '../schema.dart';
import 'binary_reader.dart';
import 'levels.dart';
import 'metadata_reader.dart';
import 'thrift_compact.dart';
import 'value_codec.dart';

final class ParquetReaderImpl implements ParquetReader {
  ParquetReaderImpl._({
    required ParquetInput input,
    required ParquetFileMetadata metadata,
    required ParquetReadOptions options,
  }) : _input = input,
       _metadata = metadata,
       _options = options;

  final ParquetInput _input;
  final ParquetFileMetadata _metadata;
  final ParquetReadOptions _options;

  static Future<ParquetReaderImpl> open(
    ParquetInput input, {
    required ParquetReadOptions options,
  }) async {
    final metadata = await readParquetMetadata(input);
    return ParquetReaderImpl._(
      input: input,
      metadata: metadata,
      options: options,
    );
  }

  @override
  ParquetSchema get schema => _metadata.schema;

  @override
  ParquetFileMetadata get metadata => _metadata;

  @override
  Stream<ParquetRow> readRows() async* {
    final projectedFields = _options.columns == null
        ? schema.fields
        : schema.fields
              .where((field) => _options.columns!.contains(field.name))
              .toList();

    for (final field in projectedFields) {
      if (field.type is! ParquetPrimitiveType ||
          field.repetition == ParquetRepetition.repeated) {
        throw ParquetUnsupportedError(
          'v1 row reading only supports flat primitive fields',
        );
      }
    }

    for (final rowGroup in _metadata.rowGroups) {
      final columns = <String, List<Object?>>{};
      for (final field in projectedFields) {
        final columnMetadata = rowGroup.columns.firstWhere(
          (column) =>
              column.path.length == 1 && column.path.single == field.name,
          orElse: () => throw ParquetFormatException(
            'Row group is missing column `${field.name}`',
          ),
        );
        columns[field.name] = await _readColumn(field, columnMetadata);
      }

      for (var rowIndex = 0; rowIndex < rowGroup.rowCount; rowIndex++) {
        final row = <String, Object?>{};
        for (final entry in columns.entries) {
          row[entry.key] = entry.value[rowIndex];
        }
        yield row;
      }
    }
  }

  Future<List<Object?>> _readColumn(
    ParquetField field,
    ParquetColumnChunkMetadata columnMetadata,
  ) async {
    if (columnMetadata.compression != ParquetCompression.uncompressed) {
      throw ParquetUnsupportedError(
        'Compression `${columnMetadata.compression.name}` is not supported in v1 row reads',
      );
    }
    if (columnMetadata.dictionaryPageOffset != null) {
      throw const ParquetUnsupportedError(
        'Dictionary pages are not supported in v1 row reads',
      );
    }

    final chunkStart =
        columnMetadata.fileOffset ??
        columnMetadata.dataPageOffset ??
        (throw const ParquetFormatException('Column chunk missing offsets'));
    final chunkEnd = chunkStart + columnMetadata.totalCompressedSize;
    final chunkBytes = await _input.readRange(chunkStart, chunkEnd);
    final chunkReader = BinaryReader(chunkBytes);
    final values = <Object?>[];
    while (!chunkReader.isEOF && values.length < columnMetadata.valueCount) {
      final pageHeaderLength = _findPageHeaderLength(
        chunkBytes,
        chunkReader.offset,
      );
      final pageHeaderReader = ThriftCompactReader(
        Uint8List.sublistView(
          chunkBytes,
          chunkReader.offset,
          chunkReader.offset + pageHeaderLength,
        ),
      );
      final pageHeader = _readPageHeader(pageHeaderReader);
      chunkReader.offset += pageHeaderLength;
      final pageBytes = chunkReader.readBytes(pageHeader.compressedPageSize);
      if (pageHeader.type != _PageType.dataPage) {
        throw ParquetUnsupportedError(
          'Unsupported page type `${pageHeader.type.name}`',
        );
      }
      if (pageHeader.dataPageHeader == null) {
        throw const ParquetFormatException(
          'Data page is missing a DataPageHeader',
        );
      }
      final pageValues = _decodeDataPage(
        field,
        pageHeader.dataPageHeader!,
        pageBytes,
      );
      values.addAll(pageValues);
    }
    return values.take(columnMetadata.valueCount).toList();
  }

  List<Object?> _decodeDataPage(
    ParquetField field,
    _DataPageHeader header,
    Uint8List pageBytes,
  ) {
    if (header.encoding != _Encoding.plain) {
      throw ParquetUnsupportedError(
        'Encoding `${header.encoding.name}` is not supported in v1 row reads',
      );
    }
    if (header.repetitionLevelEncoding != _Encoding.rle ||
        header.definitionLevelEncoding != _Encoding.rle) {
      throw const ParquetUnsupportedError(
        'Only RLE definition/repetition levels are supported in v1',
      );
    }

    final primitive = field.type as ParquetPrimitiveType;
    final bitWidth = field.repetition == ParquetRepetition.optional ? 1 : 0;
    final definitionLevels = bitWidth == 0
        ? List<int>.filled(header.numValues, 0)
        : decodeDefinitionLevels(
            pageBytes,
            bitWidth: bitWidth,
            valueCount: header.numValues,
          );
    final valuesOffset = bitWidth == 0
        ? 0
        : ByteData.sublistView(pageBytes).getInt32(0, Endian.little) + 4;
    final nonNullValueCount = definitionLevels
        .where((level) => level == bitWidth)
        .length;
    final decodedValues = decodePlainValues(
      primitive,
      Uint8List.sublistView(pageBytes, valuesOffset),
      nonNullValueCount,
    );

    var valueIndex = 0;
    return List<Object?>.generate(header.numValues, (index) {
      if (definitionLevels[index] != bitWidth) {
        return null;
      }
      return decodedValues[valueIndex++];
    });
  }

  @override
  Future<void> close() async {}
}

int _findPageHeaderLength(Uint8List data, int start) {
  for (var end = start + 1; end <= data.length; end++) {
    try {
      final reader = ThriftCompactReader(
        Uint8List.sublistView(data, start, end),
      );
      _readPageHeader(reader);
      if (reader.remaining == 0) {
        return end - start;
      }
    } on ParquetException {
      continue;
    } on RangeError {
      continue;
    }
  }
  throw const ParquetFormatException('Unable to locate page header boundary');
}

_PageHeader _readPageHeader(ThriftCompactReader reader) {
  _PageType? type;
  var uncompressedPageSize = 0;
  var compressedPageSize = 0;
  _DataPageHeader? dataPageHeader;
  var previousFieldId = 0;
  while (true) {
    final field = reader.readFieldHeader(previousFieldId);
    if (field.type == ThriftType.stop) {
      break;
    }
    switch (field.id) {
      case 1:
        type = _PageType.fromId(reader.readI32());
      case 2:
        uncompressedPageSize = reader.readI32();
      case 3:
        compressedPageSize = reader.readI32();
      case 5:
        dataPageHeader = _readDataPageHeader(reader);
      case 8:
        throw const ParquetUnsupportedError(
          'DataPageV2 is not supported in v1',
        );
      default:
        reader.skip(field.type);
    }
    previousFieldId = field.id;
  }
  return _PageHeader(
    type:
        type ??
        (throw const ParquetFormatException('Page header missing type')),
    uncompressedPageSize: uncompressedPageSize,
    compressedPageSize: compressedPageSize,
    dataPageHeader: dataPageHeader,
  );
}

_DataPageHeader _readDataPageHeader(ThriftCompactReader reader) {
  var numValues = 0;
  _Encoding? encoding;
  _Encoding? definitionLevelEncoding;
  _Encoding? repetitionLevelEncoding;
  var previousFieldId = 0;
  while (true) {
    final field = reader.readFieldHeader(previousFieldId);
    if (field.type == ThriftType.stop) {
      break;
    }
    switch (field.id) {
      case 1:
        numValues = reader.readI32();
      case 2:
        encoding = _Encoding.fromId(reader.readI32());
      case 3:
        definitionLevelEncoding = _Encoding.fromId(reader.readI32());
      case 4:
        repetitionLevelEncoding = _Encoding.fromId(reader.readI32());
      default:
        reader.skip(field.type);
    }
    previousFieldId = field.id;
  }
  return _DataPageHeader(
    numValues: numValues,
    encoding:
        encoding ??
        (throw const ParquetFormatException('Data page missing encoding')),
    definitionLevelEncoding: definitionLevelEncoding ?? _Encoding.rle,
    repetitionLevelEncoding: repetitionLevelEncoding ?? _Encoding.rle,
  );
}

enum _PageType {
  dataPage(0),
  dictionaryPage(2),
  dataPageV2(3);

  const _PageType(this.id);

  final int id;

  static _PageType fromId(int id) {
    for (final value in values) {
      if (value.id == id) {
        return value;
      }
    }
    throw ParquetUnsupportedError('Unsupported page type id: $id');
  }
}

enum _Encoding {
  plain(0),
  plainDictionary(2),
  rle(3),
  bitPacked(4),
  deltaBinaryPacked(5),
  deltaLengthByteArray(6),
  deltaByteArray(7),
  rleDictionary(8),
  byteStreamSplit(9);

  const _Encoding(this.id);

  final int id;

  static _Encoding fromId(int id) {
    for (final value in values) {
      if (value.id == id) {
        return value;
      }
    }
    throw ParquetUnsupportedError('Unsupported encoding id: $id');
  }
}

final class _PageHeader {
  const _PageHeader({
    required this.type,
    required this.uncompressedPageSize,
    required this.compressedPageSize,
    required this.dataPageHeader,
  });

  final _PageType type;
  final int uncompressedPageSize;
  final int compressedPageSize;
  final _DataPageHeader? dataPageHeader;
}

final class _DataPageHeader {
  const _DataPageHeader({
    required this.numValues,
    required this.encoding,
    required this.definitionLevelEncoding,
    required this.repetitionLevelEncoding,
  });

  final int numValues;
  final _Encoding encoding;
  final _Encoding definitionLevelEncoding;
  final _Encoding repetitionLevelEncoding;
}
