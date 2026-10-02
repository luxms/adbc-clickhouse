use crate::{InfoEntry, InfoValue};
use adbc_core::{error::Result, options::InfoCode, schemas};
use arrow_array::RecordBatch;

pub(crate) struct GetInfoBuilder {
    entries: Vec<InfoEntry>,
}

impl GetInfoBuilder {
    pub fn new() -> GetInfoBuilder {
        GetInfoBuilder {
            entries: Default::default(),
        }
    }

    pub fn set_string(&mut self, code: InfoCode, string: impl Into<String>) {
        self.entries.push(InfoEntry {
            info_name: Into::<u32>::into(&code),
            info_value: Some(InfoValue::StringValue(Some(string.into()))),
        });
    }

    pub fn set_number(&mut self, code: InfoCode, number: i64) {
        self.entries.push(InfoEntry {
            info_name: Into::<u32>::into(&code),
            info_value: Some(InfoValue::Int64Value(Some(number))),
        });
    }

    pub fn finish(self) -> Result<RecordBatch> {
        let record_batch =
            serde_arrow::to_record_batch(schemas::GET_INFO_SCHEMA.fields(), &self.entries)
                .map_err(|err| {
                    adbc_core::error::Error::with_message_and_status(
                        format!("Failed to serialize catalogs: {err}"),
                        adbc_core::error::Status::Internal,
                    )
                })?;

        // serde_arrow normalizes the union field to non-nullable. Preserve the
        // exact ADBC schema without copying the underlying Arrow buffers.
        record_batch
            .with_schema(schemas::GET_INFO_SCHEMA.clone())
            .map_err(Into::into)
    }
}

impl Default for GetInfoBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{Int64Array, StringArray, UnionArray};

    use super::*;

    #[test]
    fn empty_metadata_preserves_adbc_schema() {
        let batch = GetInfoBuilder::new().finish().unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.schema().as_ref(), schemas::GET_INFO_SCHEMA.as_ref());
    }

    #[test]
    fn metadata_serializes_to_adbc_arrow59_schema() {
        let mut builder = GetInfoBuilder::new();
        builder.set_string(InfoCode::VendorName, "ClickHouse");
        builder.set_number(InfoCode::DriverAdbcVersion, 1_001_000);
        let batch = builder.finish().unwrap();
        assert_eq!(batch.schema().as_ref(), schemas::GET_INFO_SCHEMA.as_ref());
        assert_eq!(batch.num_rows(), 2);
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<UnionArray>()
            .unwrap();
        assert_eq!(values.type_id(0), 0);
        assert_eq!(values.type_id(1), 2);
        let strings = values
            .child(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let numbers = values
            .child(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(strings.value(values.value_offset(0)), "ClickHouse");
        assert_eq!(numbers.value(values.value_offset(1)), 1_001_000);
    }
}
