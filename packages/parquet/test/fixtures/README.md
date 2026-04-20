# Fixture Provenance

This directory contains a curated fixture subset for the `parquet` package test suite.

## Upstream fixtures

Source repository: `https://github.com/apache/parquet-testing`

- `upstream/valid/apache-parquet-testing-binary.parquet`
  - upstream path: `data/binary.parquet`
  - purpose: metadata decoding and file-based read coverage for a simple valid Parquet file
- `upstream/invalid/apache-parquet-testing-PARQUET-1481.parquet`
  - upstream path: `bad_data/PARQUET-1481.parquet`
  - purpose: malformed metadata regression coverage

## Local fixtures

Local fixtures are generated inside tests from the public API for round-trip and contract coverage.
