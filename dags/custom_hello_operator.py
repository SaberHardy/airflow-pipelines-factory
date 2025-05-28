from airflow.sdk import BaseOperator, Context


class HelloOperator(BaseOperator):
    def __init__(self, name, **kwargs):
        super().__init__(**kwargs)
        self.name = name

    def execute(self, context: Context):
        message = f"Hello {self.name}"
        print(f"Message: {message}, and context: {context}")
        print("===========")
        return message
