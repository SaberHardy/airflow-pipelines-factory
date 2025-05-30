from plugins.operators.notifier_operator import NotifierOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.context import Context
from airflow.utils.dates import days_ago
from airflow.sdk import BaseNotifier
from airflow import DAG


class NotifierOperator(BaseNotifier):
    def __init__(self, message, notify_target="stdout", **kwargs):
        super().__init__(**kwargs)
        self.message = message
        self.notify_target = notify_target


    def execute(self, context: Context):
        if self.notify_target == "stdout":
            print(f"Notifier: {self.message}")
        else:
            self.log.info(f"Sending message to: {self.notify_target}: {self.message}")



with DAG(
    dag_id="example_notifier_dag",
    start_date=days_ago(1),
    schedule=None,
    catchup=False,
) as dag:

    start = EmptyOperator(task_id="start")

    notify_success = NotifierOperator(
        task_id="notify_success",
        message="Data Load Complete ✅",
        notify_target="stdout",
    )

    end = EmptyOperator(task_id="end")

    start >> notify_success >> end