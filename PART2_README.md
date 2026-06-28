Source System:
--------------------
The source systems can be divided into two categories:

1. Batch sources
   - CRM
   - Billing systems
   - NetSuite / ERP


2. Streaming / near-real-time sources
   - Network telemetry
   - RAN events
   - Core network events

Architecture would look like this:
---------------------------------------

CRM / Billing / NetSuite
        |
Batch ingestion using API / JDBC / SFTP / Data Transfer
        |
Cloud Storage Raw Zone
        |
Dataflow Batch / Dataproc PySpark / BigQuery SQL
        |
BigQuery Raw → Staging → Curated → Marts


Network / RAN / Core
        |
Pub/Sub Topic
        |
Dataflow Streaming Pipeline
        |
Validation 
        |
BigQuery Raw Streaming Tables
        |
Curated Operational Tables / Analytics Marts / ML Features

Data Modeling:
----------------
I would use  dimensional modeling
For business analytics, I would create facts and dimensions

Data Quality, Lineage, and Governance:
------------------------------------------

Data quality checks

At ingestion:

  File arrived or not
  File size validation
  Schema validation
  Record count validation

At staging:

  Null checks for mandatory fields
  Data type validation
  Date and timestamp validation
  Valid status checks
  Duplicate primary key checks

At curated layer:

  Subscriber_id should be unique in dim_subscriber
  Event_id should be unique in fact_usage_event
  Usage events should map to valid subscriber_id
  Invoice amount should be non-negative
  Billing status should be valid
  Usage bytes should not be negative

Data Governance we can check authentication,data validation,data quality & data consistancy 

Security and Privacy for Subscriber and Billing Data:
-----------------------------------------------------------
IAM and access control using IAM service , integrate gcp services using service account and provide access at specific user for different use cases.
   Project-level IAM
   Dataset-level IAM
   Table-level IAM
   Authorized views
   Secret Manager
  

For PII date , we must use data masking & Use authorized views for restricted access

Cost-Aware BigQuery Design:
--------------------------------
BigQuery cost mainly comes from storage, compute, scanned bytes, and inefficient queries. I would design tables carefully with help of partitioning and clustering columns.
tables should be partitioned by date/timestamp.
cluster based on frequent filter and join columns

Query cost control can be control using below points:

  Avoid SELECT *
  Use partition filters
  Use materialized views for repeated aggregations
  Use approximate functions for large exploratory queries
  Monitor expensive queries using INFORMATION_SCHEMA
  Use Explain option to check the bigquery different stages

CI/CD for data, infrastructure-as-code, and testing:
---------------------------------------------------------

CI use any of version control tool such as github or bitbucket, and CD use jenkins to process latest build along with the QA validation.

Late-arriving, duplicate, and out-of-order data; schema evolution; and backfills:
----------------------------------------------------------------------------------
Late arriving data can be handle by increasing look back window from source side  and before ingestion into warehouse apply data deduplication.
Schema evolution from source side must use Schema registry, if any new column added can handle using null value.
Breaking change:
- Column renamed
- Data type changed
- Mandatory column removed
Baackfilling can be possible in airflow dag using catchup parameter as True.
