from datetime import datetime
from airflow.decorators import dag, task
from airflow.models import Variable

import requests

# ---

@dag(
  dag_id='tag_status_dag',
  start_date=datetime(2024, 10, 14),
  schedule='@continuous',
  max_active_runs=1,
  catchup=False
)
def tag_status_dag():
  database_ids = Variable.get("samsungskills_notion_database_ids", deserialize_json=True)
  status_tags = Variable.get("samsungskills_notion_database_status_tags", deserialize_json=True)

  notion_key = Variable.get("samsungskills_notion_key")

  @task()
  def get_incorrecly_tagged_item(label: str, formula: int):
    url = f"https://api.notion.com/v1/databases/{database_ids['tag_status']}/query"

    headers = {
      "Authorization": f"Bearer {notion_key}",
      "Notion-Version": "2022-06-28",
      "Content-Type": "application/json"
    }

    response = requests.post(url, headers=headers, json={
      'filter': {
        'and': [
          {
            'property': 'Category',
            'select': {
              'equals': 'Service breakdown'
            }
          },
          {
            'property': 'Automation-#1',
            'formula': {
              'number': {
                'equals': formula
              }
            }
          },
          {
            'property': 'Status',
            'select': {
              'does_not_equal': label
            }
          },
          {
            'property': 'Status',
            'select': {
              'does_not_equal': 'Not yet'
            }
          },
          {
            'property': 'Status',
            'select': {
              'is_not_empty': True
            }
          }
        ]
      }
    })
    
    response.raise_for_status()
    return response.json()
  
  @task.short_circuit
  def condition_is_exist(item):
    return len(item['results']) > 0
  
  @task()
  def update_item(item):
    print(item)

  # ---

  for status_name, status_tag in status_tags.items():
    incorrecly_tagged_items = get_incorrecly_tagged_item \
      .override(task_id=f"get_incorrecly_tagged_item__{status_name}") \
               (status_tag['label'], status_tag['formula'])
    
    condition_is_exist \
      .override(task_id=f"condition_is_exist__{status_name}") \
               (incorrecly_tagged_items) >> \
    update_item \
      .override(task_id=f"update_item__{status_name}") \
               (incorrecly_tagged_items)


tag_status_dag()
