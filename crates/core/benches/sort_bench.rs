use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{Criterion, criterion_group, criterion_main};
use pipeline_core::sort::{
    BatchSorter, MissingColumnAction, SignalType, SortColumnDef, SortConfig,
};
use std::sync::Arc;

fn generate_bench_batch(rows: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("service_name", DataType::Utf8, false),
        Field::new("severity_number", DataType::Int64, false),
        Field::new("timestamp", DataType::Int64, false),
    ]));

    let services = ["frontend", "backend", "checkout", "auth", "gateway"];
    let service_vec: Vec<&str> = (0..rows).map(|i| services[i % services.len()]).collect();
    let severity_vec: Vec<i64> = (0..rows)
        .map(|i| i64::try_from(i % 24).unwrap_or_default())
        .collect();
    let timestamp_vec: Vec<i64> = (0..rows)
        .map(|i| i64::try_from(rows - i).unwrap_or_default())
        .collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(service_vec)),
            Arc::new(Int64Array::from(severity_vec)),
            Arc::new(Int64Array::from(timestamp_vec)),
        ],
    )
    .unwrap()
}

fn bench_batch_sorter(c: &mut Criterion) {
    let sort_config = SortConfig {
        on_missing_column: MissingColumnAction::Skip,
        logs: vec![
            SortColumnDef::Shorthand("service_name ASC".to_string()),
            SortColumnDef::Shorthand("severity_number ASC".to_string()),
            SortColumnDef::Shorthand("timestamp ASC".to_string()),
        ],
        metrics: vec![],
        traces: vec![],
    };
    let sorter = BatchSorter::from_config(&sort_config).unwrap();

    for size in [100, 1_000, 10_000] {
        let batch = generate_bench_batch(size);
        c.bench_function(&format!("batch_sort_3_columns_{size}_rows"), |b| {
            b.iter(|| {
                sorter.sort(&batch, SignalType::Logs).unwrap();
            });
        });
    }
}

criterion_group!(benches, bench_batch_sorter);
criterion_main!(benches);
