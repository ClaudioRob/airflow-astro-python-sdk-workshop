from airflow import DAG
from datetime import datetime
from astro import sql as aql
from astro.files import File
from astro.table import Table
from astro.sql.table import Table, Metadata

# Substitua pelos seus IDs de conexão
ASTRO_S3_CONN_ID = 'minio_conn'
ASTRO_POSTGRES_CONN_ID = 'postgres_conn'

# Defina a DAG
with DAG(
    dag_id='load_minio_to_postgres',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Tarefa para carregar o arquivo do MinIO para o Postgres
    new_table = aql.load_file(
        task_id="s3_to_postgres_replace",
        input_file=File(
            path="s3://airflow/resultados3km.csv",
            conn_id=ASTRO_S3_CONN_ID,
        ),
        output_table=Table(
            name='airflow',
            metadata=Metadata(schema="corridas"),
            conn_id=ASTRO_POSTGRES_CONN_ID,
        ),
        if_exists="replace",
    )

new_table