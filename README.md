# ADBC driver for ClickHouse

Use ClickHouse through the [Arrow Database Connectivity (ADBC)](https://arrow.apache.org/adbc/)
API, with query results delivered as Apache Arrow record batches.

The goal is to give applications a common database interface for querying
ClickHouse, discovering tables and schemas, and inserting Arrow data. This is
a Rust library with an optional C-compatible driver interface—not a database
server, proxy, or command-line tool.

This is the Luxms-maintained fork of
[if0ne/adbc-clickhouse](https://github.com/if0ne/adbc-clickhouse), used by Kuboring.
It implements a subset of ADBC; see [limitations](#current-limitations) below.

## How it relates to the ClickHouse client

```text
Application → adbc-clickhouse → clickhouse-arrow → ClickHouse
              ADBC interface    Native TCP client
```

The [Luxms ClickHouse Arrow client](https://github.com/luxms/clickhouse-arrow)
handles network connections, the ClickHouse native protocol, compression, and
Arrow data transfer. This driver adds ADBC's driver/database/connection/statement
interfaces, metadata responses, and error translation.

Choose this driver when your application needs the **ADBC API**. Choose
`clickhouse-arrow` directly when you want an **async, ClickHouse-specific API**.
This wrapper is synchronous and manages its own Tokio runtime internally.

You do not need a direct dependency on the native client to use ADBC: Cargo
includes it automatically. Both libraries exchange Arrow values in-process,
so their Arrow major versions must agree. This fork uses:

| Dependency | Version / purpose |
| --- | --- |
| `adbc_core` / optional `adbc_ffi` | 0.24 — ADBC interfaces |
| Arrow | 59 — record batches and schemas |
| `serde_arrow` | 0.14 with `arrow-59` — metadata serialization |
| `luxms/clickhouse-arrow` | Exact merged-main Git revision in [Cargo.toml](Cargo.toml) |

If your application also uses the native client directly, use the same Git
revision as this driver's manifest to avoid two different client crate identities.

## Quick start

You need a running ClickHouse server with its **native TCP port** reachable,
normally `9000`. Use `host:port`, not an HTTP URL or HTTP port `8123`.

Add these dependencies to a Rust application. The Git revision below is a
tested commit on Luxms `main`; the package version remains `0.1.0`.

```toml
[dependencies]
adbc-clickhouse = { git = "https://github.com/luxms/adbc-clickhouse", rev = "ec548aa6636de468b5a8b40ec222f7f554be0f25" }
adbc_core = "0.24"
arrow-array = "59"
```

Put this in `src/main.rs`. It reads three generated rows without creating or
modifying any tables:

```rust
use adbc_clickhouse::ClickhouseDriver;
use adbc_core::{
    Connection, Database, Driver, Statement,
    options::{OptionDatabase, OptionValue},
};
use arrow_array::UInt64Array;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = std::env::var("CLICKHOUSE_ENDPOINT")
        .unwrap_or_else(|_| "127.0.0.1:9000".into());
    let username = std::env::var("CLICKHOUSE_USER")
        .unwrap_or_else(|_| "default".into());
    let password = std::env::var("CLICKHOUSE_PASSWORD").unwrap_or_default();

    let mut driver = ClickhouseDriver::default();
    let database = driver.new_database_with_opts([
        (OptionDatabase::Uri, OptionValue::String(endpoint)),
        (OptionDatabase::Username, OptionValue::String(username)),
        (OptionDatabase::Password, OptionValue::String(password)),
    ])?;
    let mut connection = database.new_connection()?;
    let mut statement = connection.new_statement()?;
    statement.set_sql_query("SELECT number AS id FROM numbers(3)")?;

    for batch in statement.execute()? {
        let batch = batch?; // Server errors may also arrive while reading.
        let ids = batch.column(0).as_any().downcast_ref::<UInt64Array>()
            .ok_or("expected a UInt64 id column")?;
        for id in ids.values() {
            println!("{id}");
        }
    }
    Ok(())
}
```

Set `CLICKHOUSE_ENDPOINT`, `CLICKHOUSE_USER`, and `CLICKHOUSE_PASSWORD` in your
environment as needed, then run `cargo run`. Expected output is `0`, `1`, and
`2`, each on its own line. These variables are read by the example, not
automatically by the library.

### Connection options

Pass string values to `new_database_with_opts`:

| Option | Meaning |
| --- | --- |
| `OptionDatabase::Uri` | Native TCP endpoint, e.g. `127.0.0.1:9000` |
| `OptionDatabase::Username` | ClickHouse username |
| `OptionDatabase::Password` | ClickHouse password |
| `OptionDatabase::Other("clickhouse.schema".into())` | ClickHouse database; defaults to `default` |

Despite its historical name, `clickhouse.schema` selects a **database**.
The constant `adbc_clickhouse::DATABASE_OPTION_SCHEMA` contains this option name.
The wrapper does not expose every native-client setting: for example, it has
no TLS configuration option. The connection in this example is unencrypted.

## Queries, metadata, and inserts

- **Queries:** use `set_sql_query` followed by `execute`; consume the returned
  reader and handle errors from each batch.
- **Metadata:** use `get_info`, `get_objects`, `get_table_types`, or
  `get_table_schema` on the connection.
- **DDL / SQL without result rows:** use `set_sql_query` and `execute_update`.
- **Arrow ingestion:** create a fresh statement without a SQL query, set
  `OptionStatement::TargetTable` through the `Optionable` trait, then call
  `bind(batch)` or `bind_stream(reader)` and `execute_update`. The target
  table must already exist and match the batch schema.

Insert completion responses are consumed before returning, so server-side
insert errors reach the caller. See the [live integration test](tests/live_clickhouse.rs)
for complete metadata, batch/stream ingestion, and error-handling examples.

## Optional shared library

```sh
cargo build --release --features ffi
```

The output is `target/release/libadbc_clickhouse.so` on Linux or
`target/release/libadbc_clickhouse.dylib` on macOS. The initialization entry point
is `ClickhouseDriverInit`; `AdbcDriverInit` is also exported. Current FFI tests
cover ADBC **1.1.0** initialization and rejection of 1.0.0.

Loading and configuration depend on your ADBC driver manager. Language-specific
driver-manager integrations are not covered by this repository's current tests.

## Current limitations

Prepared statements, parameter-schema discovery, Substrait plans, partitioned
execution, transaction commit/rollback, and statement cancellation are not
implemented. Binding Arrow data is **ingestion**, not prepared-query parameter
binding.

`execute_update` currently returns `Some(0)`, not a measured affected-row count.
Query the table if you need to verify how many rows were inserted. Do not assume
every ADBC feature is available; the source and tests define supported behavior.

## Development and tests

The repository pins **Rust 1.96.0** for builds, formatting, and linting, matching
Kuboring. With rustup installed, Cargo uses [rust-toolchain.toml](rust-toolchain.toml)
automatically.

```sh
cargo fmt --all -- --check
cargo test --all-features
cargo clippy --all-features --all-targets -- -D warnings
```

The live test is ignored by default. It creates/drops its own Memory table and
checks queries, metadata, ingestion, server errors, and recovery after a query
error. Use only a **disposable** server:

```sh
docker run --rm -d --name adbc-arrow59-test \
  -p 127.0.0.1:19000:9000 \
  -e CLICKHOUSE_USER=arrow59 -e CLICKHOUSE_PASSWORD=arrow59-test \
  -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 \
  clickhouse/clickhouse-server:latest

# Proceed only when this readiness check prints 1.
docker exec adbc-arrow59-test clickhouse-client \
  --user arrow59 --password arrow59-test --query 'SELECT 1'

ADBC_CLICKHOUSE_URI=127.0.0.1:19000 \
ADBC_CLICKHOUSE_USER=arrow59 ADBC_CLICKHOUSE_PASSWORD=arrow59-test \
  cargo test --all-features --test live_clickhouse -- --ignored

docker stop adbc-arrow59-test
```

These credentials are only for the disposable server. The live test reads
`ADBC_CLICKHOUSE_*` variables; the quick-start application reads `CLICKHOUSE_*`.
