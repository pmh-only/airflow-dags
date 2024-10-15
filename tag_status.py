from typing import Any
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable

# ---

@dag(
  start_date=datetime(2024, 10, 14),
  schedule='@continuous',
  max_active_runs=1,
  catchup=False)
def tag_status_dag():
  @task(multiple_outputs=True)
  def retrieve_variables():
    api_key = Variable.get('samsungskills_notion_key')
    database_ids = Variable.get('samsungskills_notion_database_ids', deserialize_json=True)
    target_tags = Variable.get('samsungskills_notion_target_tags', deserialize_json=True)

    db_id = database_ids['plan_db']

    return {
      'api_key': api_key,
      'db_id': db_id,
      'target_tags': target_tags
    }
  
  @task()
  def find_target_records(target_tag: dict[str, Any], api_key: str, db_id: str):
    pass
  
  done_task = EmptyOperator(task_id='done_task')

  # ---

  variables = retrieve_variables()

  for idx, target_tag in enumerate(variables['target_tags']):
    retrieve_variables >> find_target_records(target_tag, variables['api_key'], variables['db_id']) >> done_task

# ---

tag_status = tag_status_dag()
