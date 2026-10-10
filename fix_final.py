with open('crates/parquet-sink/src/partition.rs', 'r') as f:
    lines = f.readlines()

# keep up to 1161, then add the test and the closing brace
new_lines = lines[:1161]
test_content = """    }

    #[tokio::test]
    async fn test_in_flight_uploads_applies_backpressure_at_limit() {
        let op = Operator::new(Memory::default()).unwrap();
        let config = crate::config::ParquetSinkConfig {
            max_concurrent_uploads: 1, // Only 1 background upload allowed
            ..Default::default()
        };
        let mut manager = PartitionManager::new(config, op);

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("val", DataType::Int64, false),
        ]));

        let (tx, rx) = tokio::sync::oneshot::channel();
        manager.in_flight_uploads.push(tokio::spawn(async move {
            let _ = rx.await;
            Ok(())
        }));

        let b1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![
                    1_700_000_000_000_000_000,
                ])),
                Arc::new(Int64Array::from(vec![1])),
            ],
        )
        .unwrap();

        manager.route_batch(&b1, "logs").unwrap();

        // Now flush_all should fail with Max concurrent uploads reached
        let err = manager.flush_all();
        assert!(err.is_err());
        assert!(
            err.unwrap_err()
                .to_string()
                .contains("Max concurrent uploads reached")
        );

        // Release the task
        let _ = tx.send(());
    }
}
"""

with open('crates/parquet-sink/src/partition.rs', 'w') as f:
    f.writelines(new_lines)
    f.write(test_content)
