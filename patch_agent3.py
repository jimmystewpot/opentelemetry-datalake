with open('crates/parquet-sink/src/partition.rs', 'r') as f:
    content = f.read()

# insert the backpressure check
to_insert = """
        if self.in_flight_uploads.len() >= self.config.max_concurrent_uploads {
            return Err(crate::error::ParquetSinkError::Internal(
                "Max concurrent uploads reached".into(),
            ));
        }
"""
content = content.replace('active.writer.close()?;\n', 'active.writer.close()?;\n' + to_insert)

# insert the retain in check_background_errors
content = content.replace('pub fn check_background_errors(&mut self) -> Result<(), ParquetSinkError> {\n', 'pub fn check_background_errors(&mut self) -> Result<(), ParquetSinkError> {\n        self.in_flight_uploads.retain(|jh| !jh.is_finished());\n')

with open('crates/parquet-sink/src/partition.rs', 'w') as f:
    f.write(content)
