from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd

@dag(
    schedule="0 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["etl"]
)
def simple_etl():

    @task
    def extract():
        return {
            "employee": {
                "id": 1,
                "name": "Ivan",
                "department": {
                    "id": 10,
                    "name": "IT"
                }
            }
        }

    @task
    def transform(data):
        return {
            "employee_id": data["employee"]["id"],
            "employee_name": data["employee"]["name"],
            "department_id": data["employee"]["department"]["id"],
            "department_name": data["employee"]["department"]["name"]
        }

    @task
    def load(data):
        df = pd.DataFrame([data])
        print(df)

    load(transform(extract()))

simple_etl()