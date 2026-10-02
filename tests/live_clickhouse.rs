//! Explicit opt-in test: use only a disposable ClickHouse instance.
use std::{collections::HashSet, sync::Arc};

use adbc_clickhouse::{ClickhouseDriver, reader::SingleBatchReader};
use adbc_core::{
    Connection, Database, Driver, Optionable, Statement,
    options::{InfoCode, ObjectDepth, OptionDatabase, OptionStatement, OptionValue},
};
use arrow_array::{RecordBatch, StringArray, UInt64Array, UnionArray};
use arrow_schema::{DataType, Field, Schema};

#[test]
#[ignore = "requires a disposable server and ADBC_CLICKHOUSE_URI"]
fn arrow59_query_metadata_and_ingestion() {
    if let Ok(filter) = tracing_subscriber::EnvFilter::try_from_default_env() {
        let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
    }
    let uri = std::env::var("ADBC_CLICKHOUSE_URI").expect("set ADBC_CLICKHOUSE_URI");
    let username = std::env::var("ADBC_CLICKHOUSE_USER").unwrap_or_else(|_| "default".into());
    let password = std::env::var("ADBC_CLICKHOUSE_PASSWORD").unwrap_or_default();
    let mut driver = ClickhouseDriver::default();
    let db = driver
        .new_database_with_opts([
            (OptionDatabase::Uri, OptionValue::String(uri)),
            (OptionDatabase::Username, OptionValue::String(username)),
            (OptionDatabase::Password, OptionValue::String(password)),
        ])
        .unwrap();
    let mut conn = db.new_connection().unwrap();
    let table = format!("adbc_arrow59_{}", std::process::id());
    let mut ddl = conn.new_statement().unwrap();
    ddl.set_sql_query(format!(
        "CREATE TABLE {table} (id UInt64, label String, CONSTRAINT id_limit CHECK id < 10) ENGINE = Memory"
    ))
    .unwrap();
    ddl.execute_update().unwrap();

    // Both binding paths must accept the same Arrow version as the native client.
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("label", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["first", "second"])),
        ],
    )
    .unwrap();
    let mut insert = conn.new_statement().unwrap();
    insert
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String(table.clone()),
        )
        .unwrap();
    insert.bind(batch.clone()).unwrap();
    insert.execute_update().unwrap();
    let invalid_batch = RecordBatch::try_new(
        batch.schema(),
        vec![
            Arc::new(UInt64Array::from(vec![999])),
            Arc::new(StringArray::from(vec!["rejected"])),
        ],
    )
    .unwrap();
    insert
        .bind_stream(Box::new(SingleBatchReader::new(batch)))
        .unwrap();
    insert.execute_update().unwrap();
    insert.bind(invalid_batch).unwrap();
    let error = insert.execute_update().unwrap_err();
    assert!(error.message.contains("id_limit"), "{}", error.message);

    let mut query = conn.new_statement().unwrap();
    query
        .set_sql_query(format!(
            "SELECT count() AS n, sum(id) AS total FROM {table}"
        ))
        .unwrap();
    let batches = query
        .execute()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0),
        4
    );
    assert_eq!(
        batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0),
        6
    );

    let schema = conn
        .get_table_schema(None, Some("default"), &table)
        .unwrap();
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(0).data_type(), &DataType::UInt64);
    let mut info = conn
        .get_info(Some(HashSet::from([InfoCode::DriverArrowVersion])))
        .unwrap();
    assert_eq!(
        info.schema().as_ref(),
        adbc_core::schemas::GET_INFO_SCHEMA.as_ref()
    );
    let metadata = info.next().unwrap().unwrap();
    let values = metadata
        .column(1)
        .as_any()
        .downcast_ref::<UnionArray>()
        .unwrap();
    assert_eq!(values.type_id(0), 0);
    let versions = values
        .child(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(versions.value(values.value_offset(0)).starts_with("59."));
    let objects = conn
        .get_objects(
            ObjectDepth::All,
            None,
            Some("default"),
            Some(&table),
            None,
            None,
        )
        .unwrap();
    assert_eq!(
        objects.schema().as_ref(),
        adbc_core::schemas::GET_OBJECTS_SCHEMA.as_ref()
    );
    let objects = objects.collect::<Result<Vec<_>, _>>().unwrap();
    assert!(objects.iter().any(|batch| batch.num_rows() > 0));
    let types = conn
        .get_table_types()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(types.iter().any(|batch| batch.num_rows() > 0));

    query
        .set_sql_query("SELECT missing_adbc_arrow59_column")
        .unwrap();
    let failed = match query.execute() {
        Err(_) => true,
        Ok(mut rows) => rows.any(|row| row.is_err()),
    };
    assert!(
        failed,
        "a query error must surface during execute or stream consumption"
    );
    query.set_sql_query("SELECT toUInt64(42)").unwrap();
    assert_eq!(
        query.execute().unwrap().next().unwrap().unwrap().num_rows(),
        1
    );
    ddl.set_sql_query(format!("DROP TABLE {table}")).unwrap();
    ddl.execute_update().unwrap();
}
