import time
import requests
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, TableAlreadyExistsError

CATALOG_URI = "http://localhost:8181"
S3_ENDPOINT = "http://localhost:9000"

def wait_for_catalog(url, timeout=60):
    start = time.time()
    print(f"Waiting for REST catalog at {url} to be ready...")
    while time.time() - start < timeout:
        try:
            response = requests.get(f"{url}/v1/config", timeout=2)
            if response.status_code == 200:
                print("REST catalog is ready!")
                return True
        except requests.RequestException:
            pass
        time.sleep(1)
    raise TimeoutError(f"REST catalog did not become ready at {url} within {timeout} seconds.")

def main():
    wait_for_catalog(CATALOG_URI)

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

    # 1. Create namespace
    try:
        catalog.create_namespace("default")
        print("Created namespace 'default'")
    except NamespaceAlreadyExistsError:
        print("Namespace 'default' already exists")

    # 2. Define schemas using pyarrow
    logs_schema = pa.schema([
        ("timestamp", pa.timestamp("us")),
        ("observed_timestamp", pa.timestamp("us")),
        ("severity_number", pa.int32()),
        ("severity_text", pa.string()),
        ("body", pa.string()),
        ("trace_id", pa.string()),
        ("span_id", pa.string()),
        ("flags", pa.uint32()),
        ("attributes", pa.string()),
        ("service_name", pa.string()),
        ("resource_attributes", pa.string()),
        ("scope_name", pa.string()),
        ("scope_version", pa.string()),
    ])

    metrics_schema = pa.schema([
        ("name", pa.string()),
        ("description", pa.string()),
        ("unit", pa.string()),
        ("timestamp", pa.timestamp("us")),
        ("value", pa.float64()),
        ("attributes", pa.string()),
        ("service_name", pa.string()),
        ("resource_attributes", pa.string()),
        ("scope_name", pa.string()),
        ("scope_version", pa.string()),
    ])

    traces_schema = pa.schema([
        ("trace_id", pa.string()),
        ("span_id", pa.string()),
        ("trace_state", pa.string()),
        ("parent_span_id", pa.string()),
        ("name", pa.string()),
        ("kind", pa.int32()),
        ("timestamp", pa.timestamp("us")),
        ("end_time", pa.timestamp("us")),
        ("attributes", pa.string()),
        ("service_name", pa.string()),
        ("resource_attributes", pa.string()),
        ("scope_name", pa.string()),
        ("scope_version", pa.string()),
        ("status_code", pa.int32()),
        ("status_message", pa.string()),
    ])

    # 3. Create tables
    tables_to_create = [
        ("default.logs", logs_schema),
        ("default.metrics", metrics_schema),
        ("default.traces", traces_schema),
    ]

    for table_name, schema in tables_to_create:
        try:
            catalog.create_table(table_name, schema=schema)
            print(f"Successfully created table '{table_name}'")
        except TableAlreadyExistsError:
            print(f"Table '{table_name}' already exists")

if __name__ == "__main__":
    main()
