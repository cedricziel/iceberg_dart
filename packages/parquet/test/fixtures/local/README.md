# Local Fixtures

The `parquet` package keeps its round-trip and API contract fixtures local to the test suite.

For v1, these fixtures are generated directly inside tests through the public writer API rather than being stored as binary files. This keeps the local fixture set small and makes the tested schema, rows, and expected behavior visible next to each test.
