use std::{
    collections::HashSet,
    sync::{Arc, LazyLock},
};

use adbc_core::{
    Connection, Optionable,
    error::{Error, Result, Status},
    options::{AdbcVersion, InfoCode, OptionConnection},
    schemas,
};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::Schema;

include!(concat!(env!("OUT_DIR"), "/deps_versions.rs"));

use crate::{
    reader::SingleBatchReader,
    statement::ClickhouseStatement,
    utils::{GetInfoBuilder, GetObjectsBuilder, NativeClientExt, Runtime, from_clickhouse_error},
};

static INFO_FIELDS: LazyLock<HashSet<InfoCode>> = LazyLock::new(|| {
    [
        InfoCode::VendorName,
        InfoCode::VendorVersion,
        InfoCode::DriverName,
        InfoCode::DriverVersion,
        InfoCode::DriverArrowVersion,
        InfoCode::DriverAdbcVersion,
    ]
    .into_iter()
    .collect()
});

pub struct ClickhouseConnection {
    rt: Arc<Runtime>,
    arrow_conn: clickhouse_arrow::ArrowClient,
    native_conn: clickhouse_arrow::NativeClient,
    clickhouse_version: String,
}

impl ClickhouseConnection {
    pub fn new(
        rt: Arc<Runtime>,
        arrow_conn: clickhouse_arrow::ArrowClient,
        native_conn: clickhouse_arrow::NativeClient,
    ) -> Self {
        let version = rt
            .block_on(native_conn.fetch_version())
            .ok()
            .flatten()
            .unwrap_or(DEP_CLICKHOUSE_ARROW_VERSION.to_string());

        Self {
            rt,
            arrow_conn,
            native_conn,
            clickhouse_version: version,
        }
    }
}

impl Optionable for ClickhouseConnection {
    type Option = OptionConnection;

    fn set_option(
        &mut self,
        key: Self::Option,
        _value: adbc_core::options::OptionValue,
    ) -> Result<()> {
        Err(Error::with_message_and_status(
            format!("[Clickhouse] Unrecognized option: {key:?}"),
            Status::NotFound,
        ))
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        Err(Error::with_message_and_status(
            format!("[Clickhouse] Unrecognized option: {key:?}"),
            Status::NotFound,
        ))
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        Err(Error::with_message_and_status(
            format!("[Clickhouse] Unrecognized option: {key:?}"),
            Status::NotFound,
        ))
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        Err(Error::with_message_and_status(
            format!("[Clickhouse] Unrecognized option: {key:?}"),
            Status::NotFound,
        ))
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        Err(Error::with_message_and_status(
            format!("[Clickhouse] Unrecognized option: {key:?}"),
            Status::NotFound,
        ))
    }
}

impl Connection for ClickhouseConnection {
    type StatementType = ClickhouseStatement;

    fn new_statement(&mut self) -> Result<Self::StatementType> {
        Ok(ClickhouseStatement::new(
            self.rt.clone(),
            self.arrow_conn.clone(),
        ))
    }

    fn cancel(&mut self) -> Result<()> {
        Err(Error::with_message_and_status(
            "[Clickhouse] Cancel not implemented".to_string(),
            Status::Internal,
        ))
    }

    fn get_info(
        &self,
        codes: Option<HashSet<adbc_core::options::InfoCode>>,
    ) -> Result<Box<dyn RecordBatchReader + Send>> {
        let codes = codes.unwrap_or_else(|| INFO_FIELDS.clone());
        let codes = codes.intersection(&INFO_FIELDS);

        let mut get_info_builder = GetInfoBuilder::new();

        codes.into_iter().for_each(|f| match f {
            InfoCode::VendorName => get_info_builder.set_string(*f, "Clickhouse"),
            InfoCode::VendorVersion => get_info_builder.set_string(*f, &self.clickhouse_version),
            InfoCode::DriverName => {
                get_info_builder.set_string(*f, "ADBC Clickhouse Driver - Rust")
            }
            InfoCode::DriverVersion => get_info_builder.set_string(*f, env!("CARGO_PKG_VERSION")),
            InfoCode::DriverArrowVersion => {
                get_info_builder.set_string(*f, DEP_ARROW_ARRAY_VERSION)
            }
            InfoCode::DriverAdbcVersion => get_info_builder.set_number(*f, {
                let raw_version: std::ffi::c_int = AdbcVersion::V110.into();
                raw_version as i64
            }),
            _ => {}
        });

        let batch = get_info_builder.finish()?;
        let reader = SingleBatchReader::new(batch);
        Ok(Box::new(reader))
    }

    fn get_objects(
        &self,
        depth: adbc_core::options::ObjectDepth,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: Option<Vec<&str>>,
        column_name: Option<&str>,
    ) -> Result<Box<dyn RecordBatchReader + Send>> {
        let builder =
            GetObjectsBuilder::new(catalog, db_schema, table_name, table_type, column_name);
        let batch = self.rt.block_on(builder.build(&self.native_conn, &depth))?;

        let reader = SingleBatchReader::new(batch);
        Ok(Box::new(reader))
    }

    fn get_table_schema(
        &self,
        _catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> Result<Schema> {
        let schema = self
            .rt
            .block_on(
                self.arrow_conn
                    .fetch_schema(db_schema, &[table_name], None, None),
            )
            .map(|schemas| {
                schemas.get(table_name).cloned().ok_or_else(|| {
                    Error::with_message_and_status(
                        format!("Failed to get schema for {table_name}"),
                        Status::Internal,
                    )
                })
            })
            .map_err(|err| from_clickhouse_error("Failed to fetch table schema", err))??;

        Ok((*schema).clone())
    }

    fn get_table_types(&self) -> Result<Box<dyn RecordBatchReader + Send>> {
        // https://github.com/ClickHouse/ClickHouse/blob/21c7dc1724d838042a8fcc5fecd19a9b14b4f93d/src/Storages/System/attachInformationSchemaTables.cpp#L84
        let table_types = vec![
            "BASE TABLE".to_string(),
            "LOCAL TEMPORARY".to_string(),
            "VIEW".to_string(),
            "SYSTEM VIEW".to_string(),
            "FOREIGN TABLE".to_string(),
        ];

        let array = arrow_array::StringArray::from(table_types);
        let batch = RecordBatch::try_new(
            schemas::GET_TABLE_TYPES_SCHEMA.clone(),
            vec![Arc::new(array)],
        )?;

        Ok(Box::new(SingleBatchReader::new(batch)))
    }

    fn get_statistic_names(&self) -> Result<Box<dyn RecordBatchReader + Send>> {
        Err(Error::with_message_and_status(
            "GetStatisticNames not implemented".to_string(),
            Status::NotImplemented,
        ))
    }

    fn get_statistics(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _approximate: bool,
    ) -> Result<Box<dyn RecordBatchReader + Send>> {
        Err(Error::with_message_and_status(
            "GetStatistics is not implemented".to_string(),
            Status::NotImplemented,
        ))
    }

    fn commit(&mut self) -> Result<()> {
        Err(Error::with_message_and_status(
            "Commit is not implemented".to_string(),
            Status::NotImplemented,
        ))
    }

    fn rollback(&mut self) -> Result<()> {
        Err(Error::with_message_and_status(
            "Rollback is not implemented".to_string(),
            Status::NotImplemented,
        ))
    }

    fn read_partition(
        &self,
        _partition: impl AsRef<[u8]>,
    ) -> Result<Box<dyn RecordBatchReader + Send>> {
        Err(Error::with_message_and_status(
            "ReadPartition is not implemented".to_string(),
            Status::NotImplemented,
        ))
    }
}
