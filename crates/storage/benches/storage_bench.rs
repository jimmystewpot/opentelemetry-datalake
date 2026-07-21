use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;
use std::sync::Arc;
use storage::iceberg::{IcebergSink, IcebergSinkConfig, partition_batch};
use storage::{PartitionGranularity, SchemaMode};

fn make_logs_batch_size(n: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("severity_text", DataType::Utf8, false),
        Field::new("service_name", DataType::Utf8, false),
        Field::new("resource_attributes", DataType::Utf8, false),
    ]));

    let mut ts_vec = Vec::with_capacity(n);
    let mut sev_vec = Vec::with_capacity(n);
    let mut svc_vec = Vec::with_capacity(n);
    let mut res_vec = Vec::with_capacity(n);

    for i in 0..n {
        ts_vec.push(1_717_891_200_000_000_000 + (i as i64 * 100_000_000) % 86_400_000_000_000);
        let sev = match i % 3 {
            0 => "INFO",
            1 => "WARN",
            _ => "ERROR",
        };
        sev_vec.push(sev);
        let svc = match i % 4 {
            0 => "service-a",
            1 => "service-b",
            2 => "service-c",
            _ => "service-d",
        };
        svc_vec.push(svc);
        res_vec.push(format!(r#"{{"service.name":"{svc}"}}"#));
    }

    let ts_array = Arc::new(arrow::array::TimestampNanosecondArray::from(ts_vec)) as ArrayRef;
    let sev_array = Arc::new(arrow::array::StringArray::from(sev_vec)) as ArrayRef;
    let svc_array = Arc::new(arrow::array::StringArray::from(svc_vec)) as ArrayRef;
    let res_array = Arc::new(arrow::array::StringArray::from(res_vec)) as ArrayRef;

    RecordBatch::try_new(schema, vec![ts_array, sev_array, svc_array, res_array]).unwrap()
}

fn make_traces_batch_size(n: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("name", DataType::Utf8, false),
        Field::new("service_name", DataType::Utf8, false),
    ]));

    let mut ts_vec = Vec::with_capacity(n);
    let mut name_vec = Vec::with_capacity(n);
    let mut svc_vec = Vec::with_capacity(n);

    for i in 0..n {
        ts_vec.push(1_717_891_200_000_000_000 + (i as i64 * 100_000_000) % 86_400_000_000_000);
        name_vec.push(format!("span-{i}"));
        let svc = match i % 4 {
            0 => "service-a",
            1 => "service-b",
            2 => "service-c",
            _ => "service-d",
        };
        svc_vec.push(svc);
    }

    let ts_array = Arc::new(arrow::array::TimestampNanosecondArray::from(ts_vec)) as ArrayRef;
    let name_array = Arc::new(arrow::array::StringArray::from(name_vec)) as ArrayRef;
    let svc_array = Arc::new(arrow::array::StringArray::from(svc_vec)) as ArrayRef;

    RecordBatch::try_new(schema, vec![ts_array, name_array, svc_array]).unwrap()
}

fn make_metrics_batch_size(n: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("name", DataType::Utf8, false),
        Field::new("service_name", DataType::Utf8, false),
        Field::new("attributes", DataType::Utf8, false),
    ]));

    let mut ts_vec = Vec::with_capacity(n);
    let mut name_vec = Vec::with_capacity(n);
    let mut svc_vec = Vec::with_capacity(n);
    let mut attr_vec = Vec::with_capacity(n);

    for i in 0..n {
        ts_vec.push(1_717_891_200_000_000_000 + (i as i64 * 100_000_000) % 86_400_000_000_000);
        name_vec.push(format!("metric-{i}"));
        let svc = match i % 4 {
            0 => "service-a",
            1 => "service-b",
            2 => "service-c",
            _ => "service-d",
        };
        svc_vec.push(svc);
        attr_vec.push(format!(r#"{{"http.status":{}}}"#, 200 + i % 5));
    }

    let ts_array = Arc::new(arrow::array::TimestampNanosecondArray::from(ts_vec)) as ArrayRef;
    let name_array = Arc::new(arrow::array::StringArray::from(name_vec)) as ArrayRef;
    let svc_array = Arc::new(arrow::array::StringArray::from(svc_vec)) as ArrayRef;
    let attr_array = Arc::new(arrow::array::StringArray::from(attr_vec)) as ArrayRef;

    RecordBatch::try_new(schema, vec![ts_array, name_array, svc_array, attr_array]).unwrap()
}

fn make_dry_run_config(schema_mode: SchemaMode) -> IcebergSinkConfig {
    IcebergSinkConfig {
        warehouse: "s3://wh/".to_string(),
        table_identifier: "db.tbl".to_string(),
        schema_mode,
        dry_run: true,
        ..Default::default()
    }
}

fn build_mock_table(runtime: iceberg::Runtime) -> iceberg::table::Table {
    let metadata_json = r#"{
      "format-version": 2,
      "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
      "location": "s3://bucket/test/location",
      "last-sequence-number": 0,
      "last-updated-ms": 1602638573000,
      "last-column-id": 4,
      "current-schema-id": 0,
      "schemas": [
        {
          "type": "struct",
          "schema-id": 0,
          "fields": [
            { "id": 1, "name": "timestamp", "required": true, "type": "timestamp_ns" },
            { "id": 2, "name": "severity_text", "required": false, "type": "string" },
            { "id": 4, "name": "service_name", "required": false, "type": "string" },
            { "id": 3, "name": "resource_attributes", "required": false, "type": "string" }
          ]
        }
      ],
      "default-spec-id": 0,
      "partition-specs": [{ "spec-id": 0, "fields": [] }],
      "last-partition-id": 999,
      "default-sort-order-id": 0,
      "sort-orders": [{ "order-id": 0, "fields": [] }],
      "properties": {},
      "current-snapshot-id": -1,
      "snapshots": [],
      "snapshot-log": [],
      "metadata-log": []
    }"#;

    let metadata: iceberg::spec::TableMetadata = serde_json::from_str(metadata_json).unwrap();
    iceberg::table::Table::builder()
        .metadata(metadata)
        .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
        .identifier(iceberg::TableIdent::from_strs(["ns1", "test1"]).unwrap())
        .file_io(iceberg::io::FileIO::new_with_memory())
        .runtime(runtime)
        .build()
        .unwrap()
}

fn bench_storage(c: &mut Criterion) {
    let tokio_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let iceberg_rt = iceberg::Runtime::new(&tokio_rt);

    let sink_fixed = IcebergSink::new(make_dry_run_config(SchemaMode::Fixed));
    let sink_catalog = IcebergSink::new(make_dry_run_config(SchemaMode::Catalog));
    let mock_table = build_mock_table(iceberg_rt);

    // 1. Benchmark record batch sorting (size: 1000 records)
    let logs_batch = make_logs_batch_size(1000);
    let traces_batch = make_traces_batch_size(1000);
    let metrics_batch = make_metrics_batch_size(1000);

    c.bench_function("sort_logs_1000_records", |b| {
        b.iter(|| sink_fixed.sort_logs(black_box(&logs_batch)).unwrap());
    });

    c.bench_function("sort_traces_1000_records", |b| {
        b.iter(|| sink_fixed.sort_traces(black_box(&traces_batch)).unwrap());
    });

    c.bench_function("sort_metrics_1000_records", |b| {
        b.iter(|| sink_fixed.sort_metrics(black_box(&metrics_batch)).unwrap());
    });

    // 2. Benchmark partition grouping (size: 1000 records)
    c.bench_function("partition_batch_hourly_1000_records", |b| {
        b.iter(|| {
            partition_batch(
                black_box(&logs_batch),
                black_box("timestamp"),
                black_box(PartitionGranularity::Hourly),
            )
            .unwrap()
        });
    });

    // 3. Benchmark Schema Mode checks
    c.bench_function("apply_schema_mode_fixed_1000_records", |b| {
        b.iter(|| {
            sink_fixed
                .apply_schema_mode(black_box(logs_batch.clone()), Some(&mock_table))
                .unwrap()
        });
    });

    c.bench_function("apply_schema_mode_catalog_1000_records", |b| {
        b.iter(|| {
            sink_catalog
                .apply_schema_mode(black_box(logs_batch.clone()), Some(&mock_table))
                .unwrap()
        });
    });
}

criterion_group!(benches, bench_storage);
criterion_main!(benches);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_mock_table_success() {
        let tokio_rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let iceberg_rt = iceberg::Runtime::new(&tokio_rt);
        let table = build_mock_table(iceberg_rt);
        assert_eq!(table.identifier().name(), "test1");
    }
}
