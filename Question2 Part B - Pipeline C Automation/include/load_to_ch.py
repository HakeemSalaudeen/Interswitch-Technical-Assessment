import clickhouse_connect
import logging

log = logging.getLogger(__name__)

def _load_to_ch(**context):
        result = context['ti'].xcom_pull(task_ids="extract_data")
        log.info(f"Loading {len(result)} records to ClickHouse")

        try:
            client = clickhouse_connect.get_client(
                host="clickhouse",
                port=8123,
                username="default",
                password="secret123"
            )

            # Create table with NULLABLE columns
            client.command("""
            CREATE TABLE IF NOT EXISTS weekend_report(
                month String,
                sat_mean_trip_count Nullable(Float64),
                sat_mean_fare Nullable(Float64),
                sat_mean_duration Nullable(Float64),
                sun_mean_trip_count Nullable(Float64),
                sun_mean_fare Nullable(Float64),
                sun_mean_duration Nullable(Float64)
            ) ENGINE = MergeTree()
            ORDER BY month
            """)
            log.info("Table created or already exists")

            # Insert data directly (no need to clean NULLs)
            client.insert("weekend_report", result)
            log.info(f"Successfully loaded {len(result)} rows into ClickHouse")

        except Exception as e:
            log.error(f"Failed to load data to ClickHouse: {str(e)}")
            raise