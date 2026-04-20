import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'internal/writer_impl.dart';
import 'options.dart';
import 'schema.dart';
import 'writer.dart';

final class ParquetOutput {
  const ParquetOutput._();

  static Future<ParquetWriter> openFile(
    String path, {
    required ParquetSchema schema,
    ParquetWriteOptions options = const ParquetWriteOptions(),
  }) async {
    final sink = File(path).openWrite();
    return ParquetWriterImpl.toSink(sink, schema: schema, options: options);
  }

  static Future<ParquetWriter> openSink(
    StreamSink<List<int>> sink, {
    required ParquetSchema schema,
    ParquetWriteOptions options = const ParquetWriteOptions(),
  }) async {
    return ParquetWriterImpl.toSink(sink, schema: schema, options: options);
  }

  static Future<Uint8List> writeToBytes(
    Iterable<ParquetRow> rows, {
    required ParquetSchema schema,
    ParquetWriteOptions options = const ParquetWriteOptions(),
  }) async {
    final sink = _BytesSink();
    final writer = await ParquetWriterImpl.toSink(
      sink,
      schema: schema,
      options: options,
    );
    await writer.writeRows(rows);
    await writer.close();
    return sink.takeBytes();
  }
}

final class _BytesSink implements StreamSink<List<int>> {
  final BytesBuilder _builder = BytesBuilder(copy: false);
  final Completer<void> _done = Completer<void>();
  bool _closed = false;

  @override
  void add(List<int> data) {
    _builder.add(data);
  }

  @override
  void addError(Object error, [StackTrace? stackTrace]) {
    if (!_done.isCompleted) {
      _done.completeError(error, stackTrace);
    }
  }

  @override
  Future<void> addStream(Stream<List<int>> stream) async {
    await for (final chunk in stream) {
      add(chunk);
    }
  }

  @override
  Future<void> close() async {
    _closed = true;
    if (!_done.isCompleted) {
      _done.complete();
    }
  }

  @override
  Future<void> get done => _done.future;

  Uint8List takeBytes() {
    if (!_closed) {
      throw StateError('Bytes are only available after the sink is closed');
    }
    return _builder.takeBytes();
  }
}
