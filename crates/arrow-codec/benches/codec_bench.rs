use arrow_codec::{decode_logs, decode_metrics, decode_traces};
use criterion::{Criterion, criterion_group, criterion_main};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use std::hint::black_box;

fn bench_decoders(c: &mut Criterion) {
    c.bench_function("decode_logs_empty", |b| {
        let req = ExportLogsServiceRequest::default();
        b.iter(|| decode_logs(black_box(&req)).unwrap());
    });

    c.bench_function("decode_traces_empty", |b| {
        let req = ExportTraceServiceRequest::default();
        b.iter(|| decode_traces(black_box(&req)).unwrap());
    });

    c.bench_function("decode_metrics_empty", |b| {
        let req = ExportMetricsServiceRequest::default();
        b.iter(|| decode_metrics(black_box(&req)).unwrap());
    });
}

criterion_group!(benches, bench_decoders);
criterion_main!(benches);
