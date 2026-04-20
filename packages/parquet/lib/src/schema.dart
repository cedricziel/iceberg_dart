typedef ParquetRow = Map<String, Object?>;

enum ParquetRepetition { required, optional, repeated }

enum ParquetPhysicalType {
  boolean,
  int32,
  int64,
  int96,
  float,
  doubleType,
  byteArray,
  fixedLenByteArray,
}

enum ParquetTimeUnit { millis, micros, nanos }

sealed class ParquetLogicalType {
  const ParquetLogicalType();
}

final class ParquetStringType extends ParquetLogicalType {
  const ParquetStringType();
}

final class ParquetDateType extends ParquetLogicalType {
  const ParquetDateType();
}

final class ParquetTimestampType extends ParquetLogicalType {
  const ParquetTimestampType({
    required this.unit,
    required this.isAdjustedToUtc,
  });

  final ParquetTimeUnit unit;
  final bool isAdjustedToUtc;
}

final class ParquetIntType extends ParquetLogicalType {
  const ParquetIntType({required this.bitWidth, required this.isSigned});

  final int bitWidth;
  final bool isSigned;
}

final class ParquetDecimalType extends ParquetLogicalType {
  const ParquetDecimalType({required this.precision, required this.scale});

  final int precision;
  final int scale;
}

final class ParquetListType extends ParquetLogicalType {
  const ParquetListType();
}

final class ParquetMapType extends ParquetLogicalType {
  const ParquetMapType();
}

sealed class ParquetType {
  const ParquetType();
}

final class ParquetPrimitiveType extends ParquetType {
  const ParquetPrimitiveType({
    required this.physicalType,
    this.logicalType,
    this.typeLength,
    this.precision,
    this.scale,
  });

  final ParquetPhysicalType physicalType;
  final ParquetLogicalType? logicalType;
  final int? typeLength;
  final int? precision;
  final int? scale;
}

final class ParquetGroupType extends ParquetType {
  const ParquetGroupType({required this.fields, this.logicalType});

  final List<ParquetField> fields;
  final ParquetLogicalType? logicalType;
}

final class ParquetField {
  const ParquetField({
    required this.name,
    required this.repetition,
    required this.type,
    this.fieldId,
    this.metadata = const <String, String>{},
  });

  const ParquetField.required({
    required String name,
    required ParquetType type,
    int? fieldId,
    Map<String, String> metadata = const <String, String>{},
  }) : this(
         name: name,
         repetition: ParquetRepetition.required,
         type: type,
         fieldId: fieldId,
         metadata: metadata,
       );

  const ParquetField.optional({
    required String name,
    required ParquetType type,
    int? fieldId,
    Map<String, String> metadata = const <String, String>{},
  }) : this(
         name: name,
         repetition: ParquetRepetition.optional,
         type: type,
         fieldId: fieldId,
         metadata: metadata,
       );

  const ParquetField.repeated({
    required String name,
    required ParquetType type,
    int? fieldId,
    Map<String, String> metadata = const <String, String>{},
  }) : this(
         name: name,
         repetition: ParquetRepetition.repeated,
         type: type,
         fieldId: fieldId,
         metadata: metadata,
       );

  final String name;
  final ParquetRepetition repetition;
  final ParquetType type;
  final int? fieldId;
  final Map<String, String> metadata;

  bool get isPrimitive => type is ParquetPrimitiveType;
}

final class ParquetSchema {
  const ParquetSchema(this.fields, {this.name = 'schema'});

  final String name;
  final List<ParquetField> fields;
}
