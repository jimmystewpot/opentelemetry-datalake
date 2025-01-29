use arrow::array::{Array, ArrayRef};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use opentelemetry_semantic_conventions as semconv;
use pipeline_core::error::PipelineError;
use std::collections::HashMap;
use std::sync::Arc;

use crate::common::downcast_string_array;

/// Defines the strategy for handling non-compliant records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ComplianceMode {
    /// Errors out immediately on non-compliant payloads.
    Strict,
    /// Routes non-compliant batches to quarantine.
    Quarantine,
    /// Attempts to remap keys to standard OpenTelemetry semantic conventions.
    Remap,
    /// Bypasses compliance checks.
    Off,
}

/// Represents the output after evaluating a batch's compliance.
#[derive(Debug)]
pub enum ComplianceOutput {
    /// The batch is fully compliant.
    Compliant(RecordBatch),
    /// The batch was remapped to be compliant.
    Remapped(RecordBatch),
    /// The batch is non-compliant and is quarantined.
    Quarantined(RecordBatch),
}

/// Validates schema metadata compliance and resource attribute presence.
pub struct ComplianceEngine {
    mode: ComplianceMode,
    mappings: HashMap<String, String>,
}

impl ComplianceEngine {
    /// Creates a new `ComplianceEngine` with the given mode and attribute mappings.
    #[must_use]
    pub fn new(mode: ComplianceMode, mappings: HashMap<String, String>) -> Self {
        Self { mode, mappings }
    }

    /// Checks if a schema contains the compliance verification metadata tag.
    #[must_use]
    pub fn is_schema_compliant(&self, schema: &Schema) -> bool {
        schema
            .metadata()
            .get("otel::compliance::status")
            .is_some_and(|v| v == "verified")
    }

    /// Checks if a batch contains the required `service.name` resource attribute.
    ///
    /// # Errors
    ///
    /// Returns `PipelineError::Internal` if the `resource_attributes` column
    /// is not a UTF-8 string array.
    pub fn check_batch_compliance(&self, batch: &RecordBatch) -> Result<bool, PipelineError> {
        let Some(col) = batch.column_by_name("resource_attributes") else {
            return Ok(false);
        };

        let arr = downcast_string_array(col.as_ref(), "resource_attributes")?;
        for i in 0..arr.len() {
            if arr.is_null(i) {
                return Ok(false);
            }
            let val = arr.value(i);
            let Ok(json_val) = serde_json::from_str::<serde_json::Value>(val) else {
                return Ok(false);
            };
            let Some(map) = json_val.as_object() else {
                return Ok(false);
            };
            if !map.contains_key(semconv::resource::SERVICE_NAME) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Injects `otel::compliance::status = "verified"` into the schema metadata.
    #[must_use]
    pub fn tag_schema_compliant(&self, schema: &Schema) -> SchemaRef {
        let mut metadata = schema.metadata().clone();
        metadata.insert(
            "otel::compliance::status".to_string(),
            "verified".to_string(),
        );
        Arc::new(Schema::new_with_metadata(schema.fields().clone(), metadata))
    }

    /// Assesses a record batch and applies compliance rules.
    ///
    /// # Errors
    ///
    /// Returns `PipelineError::Internal` if verification fails in `Strict` mode.
    pub fn assess_and_remap(&self, batch: RecordBatch) -> Result<ComplianceOutput, PipelineError> {
        if self.mode == ComplianceMode::Off {
            return Ok(ComplianceOutput::Compliant(batch));
        }

        if self.is_schema_compliant(&batch.schema()) {
            return Ok(ComplianceOutput::Compliant(batch));
        }

        match self.mode {
            ComplianceMode::Strict => {
                if self.check_batch_compliance(&batch)? {
                    let new_schema = self.tag_schema_compliant(&batch.schema());
                    let tagged_batch = RecordBatch::try_new(new_schema, batch.columns().to_vec())
                        .map_err(PipelineError::Arrow)?;
                    Ok(ComplianceOutput::Compliant(tagged_batch))
                } else {
                    Err(PipelineError::Internal(
                        "Strict compliance validation failed: missing SERVICE_NAME in resource attributes"
                            .to_string(),
                    ))
                }
            }
            ComplianceMode::Quarantine => {
                if self.check_batch_compliance(&batch)? {
                    let new_schema = self.tag_schema_compliant(&batch.schema());
                    let tagged_batch = RecordBatch::try_new(new_schema, batch.columns().to_vec())
                        .map_err(PipelineError::Arrow)?;
                    Ok(ComplianceOutput::Compliant(tagged_batch))
                } else {
                    Ok(ComplianceOutput::Quarantined(batch))
                }
            }
            ComplianceMode::Remap => {
                let remapped = self.execute_remap_rules(&batch)?;
                if self.check_batch_compliance(&remapped)? {
                    let new_schema = self.tag_schema_compliant(&remapped.schema());
                    let tagged_batch =
                        RecordBatch::try_new(new_schema, remapped.columns().to_vec())
                            .map_err(PipelineError::Arrow)?;
                    Ok(ComplianceOutput::Remapped(tagged_batch))
                } else {
                    Ok(ComplianceOutput::Quarantined(remapped))
                }
            }
            ComplianceMode::Off => Ok(ComplianceOutput::Compliant(batch)),
        }
    }

    fn execute_remap_rules(&self, batch: &RecordBatch) -> Result<RecordBatch, PipelineError> {
        let schema = batch.schema();
        let mut new_columns = Vec::new();

        for (i, field) in schema.fields().iter().enumerate() {
            let col = batch.column(i);
            if field.name() == "attributes" || field.name() == "resource_attributes" {
                let arr = downcast_string_array(col.as_ref(), field.name())?;
                let mut builder = arrow::array::StringBuilder::new();
                for j in 0..arr.len() {
                    if arr.is_null(j) {
                        builder.append_null();
                    } else {
                        let val = arr.value(j);
                        let Ok(mut json_val) = serde_json::from_str::<serde_json::Value>(val)
                        else {
                            builder.append_value(val);
                            continue;
                        };
                        if let Some(map) = json_val.as_object_mut() {
                            for (src, target) in &self.mappings {
                                if let Some(removed_val) = map.remove(src) {
                                    map.insert(target.clone(), removed_val);
                                }
                            }
                        }
                        builder.append_value(json_val.to_string());
                    }
                }
                new_columns.push(Arc::new(builder.finish()) as ArrayRef);
            } else {
                new_columns.push(col.clone());
            }
        }

        RecordBatch::try_new(schema.clone(), new_columns).map_err(PipelineError::Arrow)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{AsArray, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn make_test_batch(service_name: Option<&str>, legacy_key: Option<&str>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("attributes", DataType::Utf8, false),
            Field::new("resource_attributes", DataType::Utf8, false),
        ]));

        let resource_attrs = match service_name {
            Some(name) => format!(r#"{{"service.name":"{}"}}"#, name),
            None => r#"{}"#.to_string(),
        };

        let attrs = match legacy_key {
            Some(key) => format!(r#"{{"{}":"GET"}}"#, key),
            None => r#"{}"#.to_string(),
        };

        let attrs_array = Arc::new(StringArray::from(vec![attrs])) as ArrayRef;
        let res_array = Arc::new(StringArray::from(vec![resource_attrs])) as ArrayRef;

        RecordBatch::try_new(schema, vec![attrs_array, res_array]).unwrap()
    }

    #[test]
    fn test_compliance_engine_compliant() {
        let engine = ComplianceEngine::new(ComplianceMode::Strict, HashMap::new());
        let batch = make_test_batch(Some("my-service"), None);

        let output = engine.assess_and_remap(batch).unwrap();
        assert!(matches!(output, ComplianceOutput::Compliant(_)));

        if let ComplianceOutput::Compliant(b) = output {
            assert!(engine.is_schema_compliant(&b.schema()));
        }
    }

    #[test]
    fn test_compliance_engine_strict_fail() {
        let engine = ComplianceEngine::new(ComplianceMode::Strict, HashMap::new());
        let batch = make_test_batch(None, None);

        let res = engine.assess_and_remap(batch);
        assert!(res.is_err());
    }

    #[test]
    fn test_compliance_engine_quarantine() {
        let engine = ComplianceEngine::new(ComplianceMode::Quarantine, HashMap::new());
        let batch = make_test_batch(None, None);

        let output = engine.assess_and_remap(batch).unwrap();
        assert!(matches!(output, ComplianceOutput::Quarantined(_)));
    }

    #[test]
    fn test_compliance_engine_remap() {
        let mut mappings = HashMap::new();
        mappings.insert("custom_verb".to_string(), "http.request.method".to_string());

        let engine = ComplianceEngine::new(ComplianceMode::Remap, mappings);
        let batch = make_test_batch(Some("my-service"), Some("custom_verb"));

        let output = engine.assess_and_remap(batch).unwrap();
        assert!(matches!(output, ComplianceOutput::Remapped(_)));

        if let ComplianceOutput::Remapped(b) = output {
            let col = b.column_by_name("attributes").unwrap().as_string::<i32>();
            let val = col.value(0);
            assert!(val.contains("http.request.method"));
            assert!(!val.contains("custom_verb"));
            assert!(engine.is_schema_compliant(&b.schema()));
        }
    }
}
