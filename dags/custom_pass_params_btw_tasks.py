from airflow.decorators import dag, task
import pendulum
import random


@dag(dag_id="example_taskflow_approval_logic",
     start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
     schedule=None, catchup=False)
def approval_workflow():
    @task
    def generate_numbers():
        a = random.randint(1, 50)
        b = random.randint(1, 50)
        print(f"Generated numbers: a={a}, b={b}")
        return {"a": a, "b": b}

    @task
    def check_sum(data: dict):
        total = data["a"] + data["b"]
        approved = total > 60
        print(f"Sum: {total}, Approved: {approved}")
        return {"approved": approved, "total": total}

    @task
    def notify(result: dict):
        if result["approved"]:
            print(f"✅ Approved! Total is {result['total']}")
        else:
            print(f"❌ Rejected. Total is {result['total']}")

    numbers = generate_numbers()
    result = check_sum(numbers)
    notify(result)


approval_workflow()
