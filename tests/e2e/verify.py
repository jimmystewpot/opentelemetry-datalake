import os
import sys
import time
from pyiceberg.catalog import load_catalog

CATALOG_URI = "http://localhost:8181"
S3_ENDPOINT = "http://localhost:9000"

def check_catalog_tables(catalog):
    print("Checking catalog tables and verifying data presence...")
    MAX_BATCH_RECORDS = 10000  # From config.toml
    TOLERANCE_MULTIPLIER = 1.5  # Allow single gRPC requests to push over the limit slightly
    MAX_ALLOWED_RECORDS_PER_COMMIT = int(MAX_BATCH_RECORDS * TOLERANCE_MULTIPLIER)

    for table_name in ["default.logs", "default.metrics", "default.traces"]:
        try:
            table = catalog.load_table(table_name)
            print(f"\n--- Analyzing Table '{table_name}' ---")
            
            # Retrieve data from the table to verify actual persistence
            arrow_table = table.scan().to_arrow()
            row_count = len(arrow_table)
            print(f"Total rows: {row_count}")
            
            if row_count == 0:
                print(f"Error: Table '{table_name}' contains 0 rows. Data was not persisted!")
                return False

            snapshots = table.snapshots()
            print(f"Total snapshots/commits: {len(snapshots)}")

            if not snapshots:
                print(f"Error: No snapshots found for table '{table_name}'!")
                return False

            commits_records = []
            for i, snap in enumerate(snapshots):
                summary = snap.summary
                added_records_str = summary.get("added-records")
                if added_records_str is not None:
                    added_records = int(added_records_str)
                    commits_records.append(added_records)
                    print(f"  Commit #{i+1}: Snapshot ID {snap.snapshot_id}, added {added_records} records (operation: {summary.get('operation')})")
                else:
                    print(f"  Commit #{i+1}: Snapshot ID {snap.snapshot_id} (no added-records in summary)")

            if commits_records:
                max_records = max(commits_records)
                avg_records = sum(commits_records) / len(commits_records)
                print(f"Batching Stats for {table_name}: Max records/commit = {max_records}, Avg records/commit = {avg_records:.1f}")

                # Verify that max records per commit does not exceed our batch limit + tolerance
                # This guarantees that the table buffer is successfully checking max_batch_records and flushing
                if max_records > MAX_ALLOWED_RECORDS_PER_COMMIT:
                    print(f"Error: Batch sizing alignment failed! A commit has {max_records} records, which exceeds the limit of {MAX_ALLOWED_RECORDS_PER_COMMIT}")
                    return False
                
                # Also verify that batching is actually working (i.e. we aren't committing 1 record at a time)
                # With a batch size of 10,000, we expect average commit size to be reasonably large (> 1000)
                if avg_records < 500 and row_count > 10000:
                    print(f"Error: Batch size average is too small ({avg_records:.1f}). Batching is not operational!")
                    return False
            else:
                print(f"Warning: No 'added-records' metadata found in table snapshots.")

        except Exception as e:
            print(f"Error loading or querying table '{table_name}': {e}")
            return False
    return True

def verify_commits_in_log(log_path, timeout=30):
    print(f"Verifying commits in receiver log: {log_path}...")
    expected_tables = ["default.logs", "default.metrics", "default.traces"]
    
    start = time.time()
    while time.time() - start < timeout:
        if not os.path.exists(log_path):
            time.sleep(1)
            continue
            
        with open(log_path, "r") as f:
            content = f.read()
            
        missing = []
        for table in expected_tables:
            found = False
            for line in content.splitlines():
                if "Committed ACID transaction" in line and table in line:
                    found = True
                    break
            if not found:
                missing.append(table)
                
        if not missing:
            print("All commits found in receiver log!")
            return True
            
        print(f"Waiting for commits... Still missing: {missing}")
        time.sleep(2)
        
    print(f"Failed to find all commits in receiver log after {timeout} seconds.")
    if os.path.exists(log_path):
        with open(log_path, "r") as f:
            print("--- RECEIVER LOG CONTENT ---")
            print(f.read())
            print("----------------------------")
    return False

def main():
    # Initialize PyIceberg catalog
    catalog = load_catalog(
        "default",
        **{
            "type": "rest",
            "uri": CATALOG_URI,
            "s3.endpoint": S3_ENDPOINT,
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
            "s3.region": "us-east-1",
        }
    )

    log_path = os.path.join(os.path.dirname(__file__), "receiver.log")
    if not verify_commits_in_log(log_path, timeout=60):
        print("Commit verification FAILED.")
        sys.exit(1)

    if not check_catalog_tables(catalog):
        print("Catalog table validation FAILED.")
        sys.exit(1)

    print("All E2E checks passed successfully!")

if __name__ == "__main__":
    main()
