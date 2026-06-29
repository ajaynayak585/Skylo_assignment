from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from google.cloud import bigquery


PROJECT_ID = "your-gcp-project-id"
RAW_DATASET = "raw"
CURATED_DATASET = "curated"
MART_DATASET = "mart"

DATA_PATH = "/opt/airflow/data"

SUBSCRIBERS_FILE = f"{DATA_PATH}/subscribers.csv"
USAGE_EVENTS_FILE = f"{DATA_PATH}/usage_events.csv"
BILLING_FILE = f"{DATA_PATH}/billing.csv"


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def load_csv_to_bigquery(file_path, table_id):
    client = bigquery.Client(project=PROJECT_ID)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    with open(file_path, "rb") as source_file:
        load_job = client.load_table_from_file(
            source_file,
            table_id,
            job_config=job_config,
        )

    load_job.result()

    table = client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows into {table_id}")


def load_subscriber():
    table_id = f"{PROJECT_ID}.{RAW_DATASET}.raw_subscribers"
    load_csv_to_bigquery(SUBSCRIBERS_FILE, table_id)


def load_usage_events():
    table_id = f"{PROJECT_ID}.{RAW_DATASET}.raw_usage_events"
    load_csv_to_bigquery(USAGE_EVENTS_FILE, table_id)


def load_billing():
    table_id = f"{PROJECT_ID}.{RAW_DATASET}.raw_billing"
    load_csv_to_bigquery(BILLING_FILE, table_id)


def run_query(query):
    client = bigquery.Client(project=PROJECT_ID)
    job = client.query(query)
    job.result()
    print("Query completed successfully")


def dq_check():
    client = bigquery.Client(project=PROJECT_ID)

    dq_queries = {
        "duplicate_subscribers": f"""
            SELECT subscriber_id, COUNT(*) AS cnt
            FROM `{PROJECT_ID}.{RAW_DATASET}.raw_subscribers`
            GROUP BY subscriber_id
            HAVING COUNT(*) > 1
        """,

        "usage_without_subscriber": f"""
            SELECT u.*
            FROM `{PROJECT_ID}.{RAW_DATASET}.raw_usage_events` u
            LEFT JOIN `{PROJECT_ID}.{RAW_DATASET}.raw_subscribers` s
            ON u.subscriber_id = s.subscriber_id
            WHERE s.subscriber_id IS NULL
        """,

        "invalid subscriber status": f"""
            SELECT *
            FROM `{PROJECT_ID}.{RAW_DATASET}.raw_subscribers`
            WHERE status NOT IN ('active', 'suspended', 'cancelled');
        """,
    }

    failed_checks = []

    for check_name, query in dq_queries.items():
        result = client.query(query).result()
        rows = list(result)

        if len(rows) > 0:
            failed_checks.append((check_name, len(rows)))

    if failed_checks:
        error_message = "DQ checks failed: " + str(failed_checks)
        raise Exception(error_message)

    print("All DQ checks passed")


def build_curated_tables():
    query = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{CURATED_DATASET}.dim_subscriber` AS
    SELECT
        TRIM(CAST(subscriber_id AS STRING)) AS subscriber_id,
        TRIM(CAST(partner_mno AS STRING)) AS partner_mno,
        TRIM(CAST(plan_type AS STRING)) AS plan_type,
        SAFE_CAST(activation_date AS DATE) AS activation_date,
        UPPER(TRIM(region)) AS region,
        LOWER(TRIM(status)) AS status,
        CURRENT_TIMESTAMP() AS created_at
    FROM `{PROJECT_ID}.{RAW_DATASET}.raw_subscribers`
    WHERE subscriber_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY subscriber_id
        ORDER BY subscriber_id
    ) = 1;


    CREATE OR REPLACE TABLE `{PROJECT_ID}.{CURATED_DATASET}.dim_usage_event`
    PARTITION BY DATE(event_ts)
    CLUSTER BY subscriber_id, ntn_beam_id AS
    SELECT
        TRIM(CAST(event_id AS STRING)) AS event_id,
        TRIM(CAST(subscriber_id AS STRING)) AS subscriber_id,
        SAFE_CAST(event_ts AS TIMESTAMP) AS event_ts,
        TRIM(CAST(session_id AS STRING)) AS session_id,
        SAFE_CAST(bytes_up/bytes_down AS INT64) AS bytes_up_down,
        TRIM(CAST(ntn_beam_id AS STRING)) AS ntn_beam_id,
        SAFE_CAST(duration_s AS INT64) AS duration_s,
        CURRENT_TIMESTAMP() AS created_at
    FROM `{PROJECT_ID}.{RAW_DATASET}.raw_usage_events`
    WHERE event_id IS NOT NULL
      AND SAFE_CAST(event_ts AS TIMESTAMP) IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY event_id
        ORDER BY event_id
    ) = 1;


    CREATE OR REPLACE TABLE `{PROJECT_ID}.{CURATED_DATASET}.fact_partner_invoice` AS
    SELECT
        TRIM(CAST(invoice_id AS STRING)) AS invoice_id,
        TRIM(CAST(partner_mno AS STRING)) AS partner_mno,
        TRIM(CAST(billing_month AS STRING)) AS billing_month,
        SAFE_CAST(amount_usd AS NUMERIC) AS amount_usd,
        UPPER(TRIM(currency)) AS currency,
        LOWER(TRIM(status)) AS status,
        CURRENT_TIMESTAMP() AS created_at
    FROM `{PROJECT_ID}.{RAW_DATASET}.raw_billing`
    WHERE invoice_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY invoice_id
        ORDER BY invoice_id
    ) = 1;
    """

    run_query(query)


def build_mart():
    query = f"""
    CREATE OR REPLACE TABLE mart.mart_partner_monthly_usage AS
    SELECT
        s.partner_mno,
        FORMAT_TIMESTAMP('%Y-%m', u.event_ts) AS usage_month,
        COUNT(*) AS total_events,
        COUNT(DISTINCT u.subscriber_id) AS active_subscribers,
        SUM(u.bytes_up) AS total_bytes_up_down
    FROM curated.fact_usage_event u
    JOIN curated.dim_subscriber s
    ON u.subscriber_id = s.subscriber_id
    GROUP BY
        s.partner_mno,
        usage_month;
    """

    run_query(query)


with DAG(
    dag_id="csv_to_bigquery_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["bigquery", "csv", "pipeline"],
) as dag:

    load_subscribers = PythonOperator(
        task_id="load_subscribers",
        python_callable=load_subscriber,
    )

    load_usage_events = PythonOperator(
        task_id="load_usage_events",
        python_callable=load_usage_events,
    )

    load_billing_task = PythonOperator(
        task_id="load_billing",
        python_callable=load_billing,
    )

    run_dq_checks = PythonOperator(
        task_id="run_dq_checks",
        python_callable=dq_check,
    )

    build_curated = PythonOperator(
        task_id="build_curated_tables",
        python_callable=build_curated_tables,
    )

    build_marts = PythonOperator(
        task_id="build_marts",
        python_callable=build_mart,
    )

    notify = EmailOperator(
        task_id="send_email_notification",
        to=["data-platform-alerts@company.com"],
        subject="Airflow Pipeline Status: {{ dag.dag_id }} - {{ ds }}",
        """,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    [load_subscribers, load_usage_events, load_billing_task] >> run_dq_checks
    run_dq_checks >> build_curated >> build_marts >> notify
