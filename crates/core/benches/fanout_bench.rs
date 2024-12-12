use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{criterion_group, criterion_main, Criterion};
use pipeline_core::pipeline::{Fanout, SignalBatch};
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

fn criterion_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap();
    let signal = SignalBatch::Logs(batch);

    c.bench_function("fanout_send_2_outputs", |b| {
        b.to_async(&rt).iter(|| async {
            let (tx1, mut rx1) = mpsc::channel(100);
            let (tx2, mut rx2) = mpsc::channel(100);
            let fanout = Fanout::new(vec![tx1, tx2]);

            // drain the channels in the background
            tokio::spawn(async move { while rx1.recv().await.is_some() {} });
            tokio::spawn(async move { while rx2.recv().await.is_some() {} });

            fanout.send(signal.clone()).await.unwrap();
        });
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
