use std::sync::Arc;

use adbc_core::{
    Optionable, Statement, constants,
    error::{Error, Result, Status},
    options::{OptionStatement, OptionValue},
};
use arrow_array::RecordBatchReader;
use futures::TryStreamExt;

use crate::{
    reader::ClickhouseReader,
    utils::{Runtime, from_clickhouse_error},
};

pub struct ClickhouseStatement {
    rt: Arc<Runtime>,
    conn: clickhouse_arrow::ArrowClient,
    sql_query: Option<String>,
    bound_record_batch: Option<arrow_array::RecordBatch>,
    bound_record_batch_reader: Option<Box<dyn RecordBatchReader + Send>>,
    ingest_target_table: Option<String>,
}

impl ClickhouseStatement {
    async fn insert_batch(
        &self,
        target_table: &str,
        batch: arrow_array::RecordBatch,
    ) -> Result<()> {
        self.conn
            .insert(
                format!("INSERT INTO {target_table} FORMAT Native"),
                batch,
                None,
            )
            .await
            .map_err(|err| from_clickhouse_error("Failed to execute update", err))?
            // The insert stream completes the operation and delivers server errors.
            .try_for_each(|()| async { Ok(()) })
            .await
            .map_err(|err| from_clickhouse_error("Failed to execute update", err))
    }

    pub fn new(rt: Arc<Runtime>, conn: clickhouse_arrow::ArrowClient) -> Self {
        Self {
            rt,
            conn,
            sql_query: None,
            bound_record_batch: None,
            bound_record_batch_reader: None,
            ingest_target_table: None,
        }
    }
}

impl Optionable for ClickhouseStatement {
    type Option = OptionStatement;

    fn set_option(
        &mut self,
        key: Self::Option,
        value: adbc_core::options::OptionValue,
    ) -> Result<()> {
        match key.as_ref() {
            constants::ADBC_INGEST_OPTION_TARGET_TABLE => match value {
                OptionValue::String(value) => {
                    self.ingest_target_table = Some(value);
                    Ok(())
                }
                _ => Err(Error::with_message_and_status(
                    "[Clickhouse] IngestOptionTargetTable value must be of type String",
                    Status::InvalidArguments,
                )),
            },
            _ => Err(Error::with_message_and_status(
                format!("[Clickhouse] Unrecognized option: {key:?}"),
                Status::NotFound,
            )),
        }
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        match key.as_ref() {
            constants::ADBC_INGEST_OPTION_TARGET_TABLE => {
                let target_table = self.ingest_target_table.clone();
                match target_table {
                    Some(table) => Ok(table),
                    None => Err(Error::with_message_and_status(
                        format!("[Clickhouse] {key:?} has not been set"),
                        Status::NotFound,
                    )),
                }
            }
            _ => Err(Error::with_message_and_status(
                format!("[Clickhouse] Unrecognized option: {key:?}"),
                Status::NotFound,
            )),
        }
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

impl Statement for ClickhouseStatement {
    fn bind(&mut self, batch: arrow_array::RecordBatch) -> Result<()> {
        self.bound_record_batch.replace(batch);
        Ok(())
    }

    fn bind_stream(&mut self, reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        self.bound_record_batch_reader.replace(reader);
        Ok(())
    }

    fn execute(&mut self) -> Result<Box<dyn RecordBatchReader + Send>> {
        if let Some(query) = &self.sql_query {
            let response = self
                .rt
                .block_on(self.conn.query(query, None))
                .map_err(|err| from_clickhouse_error("Failed to execute query", err))?;

            Ok(Box::new(ClickhouseReader::new(self.rt.clone(), response)))
        } else {
            Err(Error::with_message_and_status(
                "[Clickhouse] SQL query is empty",
                Status::InvalidState,
            ))
        }
    }

    fn execute_update(&mut self) -> Result<Option<i64>> {
        if let Some(sql) = &self.sql_query {
            self.rt
                .block_on(self.conn.execute(sql, None))
                .map_err(|err| from_clickhouse_error("Failed to execute update", err))?;
        } else if let Some(record_batch) = self.bound_record_batch.take()
            && let Some(target_table) = &self.ingest_target_table
        {
            self.rt
                .block_on(self.insert_batch(target_table, record_batch))?;
        } else if let Some(reader) = self.bound_record_batch_reader.take()
            && let Some(target_table) = &self.ingest_target_table
        {
            self.rt.block_on(async {
                for batch in reader {
                    self.insert_batch(target_table, batch?).await?;
                }

                Result::Ok(())
            })?;
        }

        Ok(Some(0))
    }

    fn execute_schema(&mut self) -> Result<arrow_schema::Schema> {
        Err(Error::with_message_and_status(
            "[Clickhouse] ExecuteSchema not implemented".to_string(),
            Status::NotImplemented,
        ))
    }

    fn execute_partitions(&mut self) -> Result<adbc_core::PartitionedResult> {
        Err(Error::with_message_and_status(
            "[Clickhouse] ExecutePartitions not implemented".to_string(),
            Status::NotImplemented,
        ))
    }

    fn get_parameter_schema(&self) -> Result<arrow_schema::Schema> {
        Err(Error::with_message_and_status(
            "[Clickhouse] GetParameterSchema not implemented".to_string(),
            Status::NotImplemented,
        ))
    }

    fn prepare(&mut self) -> Result<()> {
        Err(Error::with_message_and_status(
            "[Clickhouse] Prepare not implemented".to_string(),
            Status::NotImplemented,
        ))
    }

    fn set_sql_query(&mut self, query: impl AsRef<str>) -> Result<()> {
        self.sql_query = Some(query.as_ref().to_string());
        Ok(())
    }

    fn set_substrait_plan(&mut self, _plan: impl AsRef<[u8]>) -> Result<()> {
        Err(Error::with_message_and_status(
            "[Clickhouse] SetSubstraitPlan not implemented".to_string(),
            Status::NotImplemented,
        ))
    }

    fn cancel(&mut self) -> Result<()> {
        Err(Error::with_message_and_status(
            "[Clickhouse] Cancel not implemented".to_string(),
            Status::NotImplemented,
        ))
    }
}
