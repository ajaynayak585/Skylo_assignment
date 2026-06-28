# Skylo_assignment
We have three real time CSV file
subscribers.csv
usage_events.csv
billing.csv
<img width="1282" height="414" alt="image" src="https://github.com/user-attachments/assets/16f5edb6-662a-4df2-ab99-387d13523657" />
<img width="1348" height="595" alt="image" src="https://github.com/user-attachments/assets/c24a40d2-0add-4b22-83ce-34eb63e25803" />
<img width="1288" height="418" alt="image" src="https://github.com/user-attachments/assets/10a0d306-9253-4b7e-a3a0-b85ff0fc3f47" />


Architecture:
------------

CSV Files
    subscribers.csv
    usage_events.csv
    billing.csv
   |
Python Ingestion Pipeline into Bigquery
   |
Raw Layer
   |
Staging Layer
   |
Curated Layer
   |
Analytics Marts
   |
Data Quality Checks

Data Modeling on BQ:
------------------------

Raw layer stores data exactly as received.
     raw_subscribers
     raw_usage_events
     raw_billing

CREATE TABLE raw.raw_subscribers (
    subscriber_id STRING,
    partner_mno STRING,
    plan_type STRING,
    activation_date STRING,
    region STRING,
    status STRING,
    ingestion_ts TIMESTAMP
);

CREATE TABLE raw.raw_usage_events (
    event_id STRING,
    subscriber_id STRING,
    event_ts TIMESTAMP,
    session_id STRING,
    bytes_up/bytes_down INT64,
    ntn_beam_id STRING,
    duration_s INT64,
    ingestion_ts TIMESTAMP
);

CREATE TABLE raw.raw_billing (
    invoice_id STRING,
    partner_mno STRING,
    billing_month STRING,
    amount_usd NUMERIC,
    currency STRING,
    status STRING,
    ingestion_ts TIMESTAMP
);

Staging layer performs light cleaning.
    stg_subscribers
    stg_usage_events
    stg_billing

    
Curated layer contains clean business entities.

    dim_subscriber
    dim_usage_event
    fact_billing

    CREATE OR REPLACE TABLE curated.dim_subscriber 
        PARTITION BY DATE(ingestion_ts)
        CLUSTER BY subscriber_id, partner_mno AS
 
     SELECT
        TRIM(subscriber_id) AS subscriber_id,
        TRIM(partner_mno) AS partner_mno,
        TRIM(plan_type) AS plan_type,
        SAFE_CAST(activation_date AS DATE) AS activation_date,
        UPPER(TRIM(region)) AS region,
        LOWER(TRIM(status)) AS status,
        CURRENT_TIMESTAMP() AS created_at
        
        FROM raw.raw_subscribers
        WHERE subscriber_id IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY subscriber_id 
            ORDER BY ingestion_ts DESC
        ) = 1;

    CREATE OR REPLACE TABLE curated.fact_usage_event
        PARTITION BY DATE(event_ts)
        CLUSTER BY subscriber_id, ntn_beam_id AS
        SELECT
            event_id,
            subscriber_id,
            SAFE_CAST(event_ts AS TIMESTAMP) AS event_ts,
            session_id,
            SAFE_CAST(bytes_up AS INT64) AS bytes_up/bytes_down,
            ntn_beam_id,
            SAFE_CAST(duration_s AS INT64) AS duration_s,
            CURRENT_TIMESTAMP() AS created_at
        FROM raw.raw_usage_events
        WHERE event_id IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY event_id 
            ORDER BY ingestion_ts DESC
        ) = 1;


    CREATE OR REPLACE TABLE curated.fact_partner_invoice 
        PARTITION BY DATE(ingestion_ts)
        CLUSTER BY invoice_id, partner_mno AS
        SELECT
            invoice_id,
            partner_mno,
            billing_month,
            SAFE_CAST(amount_usd AS NUMERIC) AS amount_usd,
            UPPER(currency) AS currency,
            LOWER(status) AS status,
            CURRENT_TIMESTAMP() AS created_at
        FROM raw.raw_billing
        WHERE invoice_id IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY invoice_id 
            ORDER BY ingestion_ts DESC
        ) = 1;


Analytics Mart Layer:
--------------------------

Create business-ready tables.
    mart_partner_monthly_usage


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

Data Quality Checks:
---------------------------
subscribers.csv checks
        subscriber_id should not be null
        subscriber_id should be unique
        activation_date should be valid date
        status should be active / suspended / cancelled
        region should be NA / EU / APAC / LATAM
        plan_type should be IoT / Direct-to-Device / Automotive

        query:
        Check duplicate subscribers
            SELECT subscriber_id, COUNT(*) AS cnt
            FROM curated.dim_subscriber
            GROUP BY subscriber_id
            HAVING COUNT(*) > 1;

        Check usage events without valid subscriber
            SELECT u.*
            FROM curated.fact_usage_event u
            LEFT JOIN curated.dim_subscriber s
            ON u.subscriber_id = s.subscriber_id
            WHERE s.subscriber_id IS NULL;
            
        Check invalid subscriber status
            SELECT *
            FROM curated.dim_subscriber
            WHERE status NOT IN ('active', 'suspended', 'cancelled');
            
usage_events.csv checks
        event_id should not be null
        event_id should be unique
        subscriber_id should exist in subscribers
        event_ts should be valid timestamp

billing.csv checks
        invoice_id should not be null
        invoice_id should be unique
        partner_mno should not be null
        billing_month should be YYYY-MM format
        amount_usd should be >= 0
        currency should not be null
        status should be paid / pending / overdue
