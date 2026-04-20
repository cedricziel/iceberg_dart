final class ParquetException implements Exception {
  const ParquetException(this.message);

  final String message;

  @override
  String toString() => 'ParquetException: $message';
}

final class ParquetFormatException extends ParquetException {
  const ParquetFormatException(super.message);
}

final class ParquetUnsupportedError extends ParquetException {
  const ParquetUnsupportedError(super.message);
}

final class ParquetValidationException extends ParquetException {
  const ParquetValidationException(super.message);
}
