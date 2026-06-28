Operations & Reflection:
-----------------------------
1.monitor pipeline health using Airflow:
----------------------------------------------
A. DAG-level monitoring
  I would monitor the Airflow DAG status using:
  
  1.Airflow UI
  2.Airflow task logs
  3.Email 
  4.Cloud Logging / Cloud Monitoring

B. Task-level monitoring

  Inside Airflow, I would monitor each task separately.
  
  Example pipeline:
  
  start
    |
  load_subscribers
    |
  load_billing_data
    |
  load_usage_events
    |
  run_data_quality_checks
    |
  build_curated_tables
    |
  build_billing_mart
    |
  notify_success_or_failure

2.What I would alert on:
----------------------------------------------
Critical alerts using trigger_rule=TriggerRule.ALL_DONE

So even if upstream tasks fail, the alert/notification task still runs.

3:Pipeline failed at 3 AM before partner billing run:
---------------------------------------------------------
If the pipeline failed at 3 AM before a 7 AM billing run, I would acknowledge the alert, check the failed Airflow task, inspect logs, 
assess whether raw/staging/curated data is complete, block downstream billing if the mart is incomplete, 
rerun only the affected task.

Lets Assume:
  Failure time: 3:00 AM
  Billing run: 7:00 AM
  Pipeline: billing_pipeline

Step 1: Acknowledge the alert immediately

First, I would acknowledge the email alert so the team knows someone is looking into it.

Acknowledged: billing_pipeline failed at build_billing_mart.
Investigating now.

Step 2: Check Airflow DAG status

In Airflow UI, I would check:

Which task failed?
Did upstream tasks succeed?
Was this a retry failure or first failure?
What was the error message?
Which billing month/run_id failed?
Are downstream tasks skipped?

4.Improvements:
-------------------
Add proper Airflow alert callbacks.

If load fails → check source file
If DQ fails → check rejected records
If Data mart fails → check BigQuery SQL/logs
If SLA fails → notify billing teams

Dag.py file: 
---------------------
-Each task must call the same function which is defined in the pipeline.py file.
    
    from datetime import datetime, timedelta
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.utils.trigger_rule import TriggerRule
    
    
    default_args = {
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=5)
    }
    
    
    with DAG(
        dag_id="billing_pipeline",
        default_args=default_args,
        start_date=datetime(2026, 06, 29),
        schedule="@daily",
        catchup=False
    ) as dag:
    
    
        load_subscribers = PythonOperator(
            task_id="load_subscribers",
            python_callable=load_subscriber
    
        )
    
        load_usage_events = PythonOpearator(
            task_id="load_usage_events",
            python_callable=load_usages_events
        )
    
        load_billing = PythonOpearator(
            task_id="load_billing",
            python_callable=load_billing
        )
    
        run_dq_checks = PythonOpearator(
            task_id="run_dq_checks",
            python_callable=dq_check
        )
    
        build_marts = PythonOpearator(
            task_id="build_marts",
            python_callable=build_mart
        )
    
        notify = PythonOpearator(
            task_id="send_notification",
            python_callable=send_notification,
            trigger_rule=TriggerRule.ALL_DONE
        )
    
      
        [load_subscribers, load_usage_events, load_billing] >> run_dq_checks
        >> build_marts >> notify
