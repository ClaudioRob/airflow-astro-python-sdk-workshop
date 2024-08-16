
from airflow.operators.python import PythonOperator
from airflow.decorators import dag
from datetime import datetime
from astro import sql as aql
from astro.files import File
from astro.table import Table
import os

# Substitua pelos seus IDs de conexão
ASTRO_S3_CONN_ID = 'aws_default'
ASTRO_POSTGRES_CONN_ID = 'postgres_conn'

def load_csv_to_postgres():
    
    # Definindo a origem do arquivo no MinIO (S3)
    minio_file = File(
        path="s3://airflow/resultados3km.csv",
        conn_id="aws_default"  # Este é o ID da conexão S3 configurado no Airflow
    )
    
    # Definindo a tabela de destino no PostgreSQL
    postgres_table = Table(
        name="resultados",
        conn_id="postgres_conn",  # Este é o ID da conexão PostgreSQL configurado no Airflow
        schema="corridas"
    )
    
    # Usando a função de transformação do Astro SDK para carregar o CSV no PostgreSQL
    aql.load_file(
        input_file=minio_file,
        output_table=postgres_table,
        if_exists="replace"  # Substitui a tabela se já existir
    )

# Definição da DAG usando o Astro SDK
@dag(
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["transfer", "minio", "postgres"],
)
def dag_load_csv_to_postgres():
    transfer_task = PythonOperator(
        task_id="load_csv_to_postgres",
        python_callable=load_csv_to_postgres,
    )

dag = dag_load_csv_to_postgres()
