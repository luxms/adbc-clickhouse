# ADBC Driver for Clickhouse

## Arrow 59 compatibility branch

This branch uses ADBC Rust 0.24, Arrow 59 and serde_arrow 0.14 with its
`arrow-59` feature. The matching ClickHouse client is maintained in
`luxms/clickhouse-arrow` at an exact merged `main` revision. All of these dependencies must
agree on the Arrow major version when passing record batches in-process.
Rust 1.96.0 is pinned to match Kuboring, including formatting and linting.

The wrapper preserves the exact ADBC `GetInfo` schema and consumes insert
response streams before reporting success, including server-side insert errors.

Run unit tests, including the FFI-enabled build, with:

```sh
cargo test --all-features
```

The live test is deliberately ignored by default. It creates and drops a
process-specific Memory table and checks queries, Arrow batch/stream ingestion,
table schemas, ADBC metadata, and recovery after a query error. Run it only on a
disposable instance:

```sh
docker run --rm -d --name adbc-arrow59-test \
  -p 127.0.0.1:19000:9000 \
  -e CLICKHOUSE_USER=arrow59 -e CLICKHOUSE_PASSWORD=arrow59-test \
  -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 \
  clickhouse/clickhouse-server:latest
# Wait for the server to become ready, then:
ADBC_CLICKHOUSE_URI=127.0.0.1:19000 \
ADBC_CLICKHOUSE_USER=arrow59 ADBC_CLICKHOUSE_PASSWORD=arrow59-test \
  cargo test --all-features --test live_clickhouse -- --ignored
docker stop adbc-arrow59-test
```

## Example usage

```rust
use adbc_core::{
    Connection, Database, Driver,
    options::{OptionDatabase, OptionValue},
};

use adbc_clickhouse::driver::ClickhouseDriver;

fn main() {
    let mut driver = ClickhouseDriver::default();
    let database = driver
        .new_database_with_opts([
            (
                OptionDatabase::Uri,
                OptionValue::String("localhost:9000".to_string()),
            ),
            (
                OptionDatabase::Username,
                OptionValue::String("username".to_string()),
            ),
            (
                OptionDatabase::Password,
                OptionValue::String("password".to_string()),
            ),
            (
                OptionDatabase::Other("clickhouse.schema".to_string()),
                OptionValue::String("default".to_string()),
            )
        ])
        .unwrap();

    let connection = database.new_connection().unwrap();
    let batch = connection
        .get_objects(
            adbc_core::options::ObjectDepth::All,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap()
        .collect::<Vec<_>>()
        .into_iter()
        .filter_map(|s| s.ok())
        .collect::<Vec<_>>();

    arrow::util::pretty::print_batches(&batch).unwrap();
}
```
