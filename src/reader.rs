use std::{pin::Pin, sync::Arc};

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, Schema};
use clickhouse_arrow::ClickHouseResponse;
use futures::{StreamExt, stream::Peekable};

use crate::utils::Runtime;

#[derive(Debug)]
pub struct SingleBatchReader {
    batch: Option<RecordBatch>,
    schema: Arc<Schema>,
}

impl SingleBatchReader {
    pub fn new(batch: RecordBatch) -> Self {
        let schema = batch.schema();
        Self {
            batch: Some(batch),
            schema,
        }
    }
}

impl Iterator for SingleBatchReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        Ok(self.batch.take()).transpose()
    }
}

impl RecordBatchReader for SingleBatchReader {
    fn schema(&self) -> arrow_schema::SchemaRef {
        self.schema.clone()
    }
}

pub struct ClickhouseReader {
    rt: Arc<Runtime>,
    stream: Pin<Box<Peekable<ClickHouseResponse<RecordBatch>>>>,
    schema: Option<Arc<arrow_schema::Schema>>,
}

impl ClickhouseReader {
    pub fn new(rt: Arc<Runtime>, stream: ClickHouseResponse<RecordBatch>) -> Self {
        let mut peekable = Box::pin(stream.peekable());
        let schema = rt
            .block_on(peekable.as_mut().peek())
            .map(|res| res.as_ref().map(|res| res.schema()))
            .transpose()
            .ok()
            .flatten();

        Self {
            rt,
            stream: peekable,
            schema,
        }
    }
}

impl Iterator for ClickhouseReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.rt.block_on(self.stream.next())?;
        Some(next.map_err(|err| ArrowError::ExternalError(Box::new(err))))
    }
}

impl RecordBatchReader for ClickhouseReader {
    fn schema(&self) -> arrow_schema::SchemaRef {
        self.schema.clone().expect("failed to fetch schema")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field};

    #[test]
    fn single_batch_reader_preserves_arrow59_values_and_schema() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![Some(42), None]))],
        )
        .unwrap();
        let mut reader: Box<dyn RecordBatchReader + Send> =
            Box::new(SingleBatchReader::new(batch.clone()));
        assert_eq!(reader.schema(), schema);
        assert_eq!(reader.next().unwrap().unwrap(), batch);
        assert!(reader.next().is_none());
        assert_eq!(reader.schema(), schema);
    }
}
