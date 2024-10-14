from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator

from datetime import datetime

# ---

dag = DAG(
  "simple_test_dag",
  default_args={"retries": 2},
  tags=["test"],
  start_date=datetime(2021, 1, 1),
  schedule="*/5 * * * *",
  catchup=False,
)

# ---

task_fetch_lorem_ipsum = HttpOperator(
  http_conn_id="lorem_ipsum_api",
  task_id="task_fetch_lorem_ipsum",
  endpoint="/api/plaintext",
  method="GET",
  log_response=True,
  dag=dag
)

# ---

task_fetch_lorem_ipsum
