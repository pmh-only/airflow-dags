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

    db_id = database_ids['plan_db']

    return {
      'api_key': api_key,
      'db_id': db_id,
    }
  
  @task()
  def find_target_records(target_tag: dict[str, Any]):
    target_tag['formula']
    pass

  @task.branch()
  def check_target_records(idx: int, ti):
    print(idx, ti)
    return 'done_task'
  
  done_task = EmptyOperator(task_id='done_task')

  # ---

  target_tags = [
    {
      "formula": 0,
      "status": "\u200f\u200f\u200e \u200e\u200f\u200e \u200e\u200f\u200f\u200e \u200e\u200f\u200f\u200e \u200e\u200f\u200f\u200e \u200e\u200f\u200f\u200e \u200e\u200f\u200f\u200e\u200eTODO\u200f\u200f\u200e \u200e\u200f\u200f\u200e \u200e\u200f\u200f\u200e \u200e\u200f\u200f\u200e \u200e\u200f\u200f\u200e\u200f\u200f\u200e \u200e"
    },
    {
      "formula": 1,
      "status": "\u200f\u200f\u200e \u200eIn Progress\u200f\u200f\u200e \u200e"
    },
    {
      "formula": 2,
      "status": "\u200f\u200f\u200e \u200e\u200f\u200f\u200e \u200e\u200f\u200f\u200e \u200e\u200f\u200f\u200e \u200e\u200f\u200f\u200e \u200e\u200f\u200f\u200e \u200eDone\u200f\u200f\u200e \u200e\u200f\u200f\u200e \u200e\u200f\u200f\u200e \u200e\u200f\u200f\u200e \u200e\u200f\u200f\u200e \u200e\u200f\u200f\u200e \u200e"
    }
  ]

  for idx, target_tag in enumerate(target_tags): 
    (
      retrieve_variables() >>
      find_target_records.override(task_id=f"find_target_records_{idx}")(target_tag) >>
      check_target_records(idx) >>
      [done_task]
    )

# ---

tag_status = tag_status_dag()
