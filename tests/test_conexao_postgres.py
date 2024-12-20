from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Definindo o DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'postgres_connection_test',
    default_args=default_args,
    description='Testando a conexão com o PostgreSQL',
    schedule_interval='@once',
    start_date=days_ago(1),
    catchup=False,
)

# Tarefa para executar uma consulta SQL
run_query = PostgresOperator(
    task_id='run_query',
    postgres_conn_id='postgres_conn',  # O ID da conexão que você configurou
    sql='SELECT version();',  # Consulta SQL para testar a conexão
    dag=dag,
)

run_query
