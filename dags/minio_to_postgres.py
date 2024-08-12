from airflow.decorators import dag
from astro import sql as aql
from astro.files import File
from astro.table import Table
from datetime import datetime

# Define o DAG usando o decorator do Airflow
@dag(
    'minio_to_postgres',
     schedule_interval=None,
     start_date=datetime(2024, 1, 1),
     catchup=False,
)

def minio_to_postgres_pipeline():
    
    # Define a tarefa para carregar dados do MinIO
    @aql.transform
    def load_data():
        return """
        SELECT *
        FROM {{input}}
        """

    # Especifica o arquivo de entrada no MinIO e a tabela de saída no PostgreSQL
    input_file = File("s3://airflow/resultados3km.csv", conn_id='minio_default')
    output_table = Table(name='vemcuidardemim', schema='corridas', conn_id='postgres_default')

    # Define a tarefa para transferir os dados
    load_data(input=input_file, output=output_table)

# Instancia o DAG
dag_instance = minio_to_postgres_pipeline()
