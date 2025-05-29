from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def create_table():
    hook = PostgresHook(postgres_conn_id='tutorial_pg_conn')
    sql_query = """
        CREATE TABLE IF NOT EXISTS airflow_users (
            id SERIAL PRIMARY KEY,
            username VARCHAR(50) NOT NULL,
            email VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """
    hook.run(sql_query)


def insert_data_into_table():
    hook = PostgresHook(postgres_conn_id='tutorial_pg_conn')
    rows = [
        ('john_doe', 'john@example.com'),
        ('jane_smith', 'jane@example.com'),
        ('bob_johnson', 'bob@example.com')
    ]
    for row in rows:
        sql = f"INSERT INTO airflow_users (username, email) VALUES {row}"
        hook.run(sql)


def query_data():
    """Query data and print results"""
    hook = PostgresHook(postgres_conn_id='tutorial_pg_conn')
    records = hook.get_records("SELECT * FROM airflow_users")
    print("Query results:")
    for row in records:
        print(row)
