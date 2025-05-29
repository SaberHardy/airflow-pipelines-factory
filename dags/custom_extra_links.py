import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import BaseOperator, DAG
from airflow.sdk import BaseOperatorLink
from airflow.models.taskinstancekey import TaskInstanceKey


class GoogleLink(BaseOperatorLink):
    name = "Google"

    def get_link(self, operator: BaseOperator, *, ti_key: TaskInstanceKey) -> str:
        return "https://www.google.com"


class DagLinkOperator(BaseOperator):
    operator_extra_links = [GoogleLink()]  # Changed to list from tuple

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def execute(self, context):
        self.log.info("Hello from Extra link")


def hello_there():
    print("hello there!!")


with DAG(
        dag_id="dag_with_extra_links",
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        schedule=None,
        catchup=False,
        tags=['example']
) as dag:
    start = PythonOperator(
        task_id="start",
        python_callable=hello_there
    )

    custom_task = PythonOperator(
        task_id="custom_task_with_link",
        python_callable=hello_there
    )

    end = PythonOperator(
        task_id="end",
        python_callable=hello_there
    )

    start >> custom_task >> end
