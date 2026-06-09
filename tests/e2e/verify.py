import os
import sys
import time
from pyiceberg.catalog import load_catalog

CATALOG_URI = "http://localhost:8181"
S3_ENDPOINT = "http://localhost:9000"

def check_catalog_tables(catalog):
    print("Checking catalog tables...")
    for table_name in ["default.logs", "default.metrics", "default.traces"]:
        try:
            table = catalog.load_table(table_name)
            print(f"Table '{table_name}' loaded successfully from catalog.")
        except Exception as e:
            print(f"Error loading table '{table_name}': {e}")
            return False
    return True

def verify_commits_in_log(log_path, timeout=30):
    print(f"Verifying commits in receiver log: {log_path}...")
    expected_logs = [
        "Committed ACID transaction for table: default.logs",
        "Committed ACID transaction for table: default.metrics",
        "Committed ACID transaction for table: default.traces"
    ]
    
    start = time.time()
    while time.time() - start < timeout:
        if not os.path.exists(log_path):
            time.sleep(1)
            continue
            
        with open(log_path, "r") as f:
            content = f.read()
            
        # Check if all expected strings are in the log content
        missing = [ex for ex in expected_logs if ex not in content]
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

    if not check_catalog_tables(catalog):
        print("Catalog table validation FAILED.")
        sys.exit(1)

    log_path = os.path.join(os.path.dirname(__file__), "receiver.log")
    if not verify_commits_in_log(log_path, timeout=60):
        print("Commit verification FAILED.")
        sys.exit(1)

    print("All E2E checks passed successfully!")

if __name__ == "__main__":
    main()
