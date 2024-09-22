from airflow.operators.python_operator import PythonOperator

def check_file_exists(**kwargs):
    path_pattern = kwargs.get('path_pattern')
    if os.path.exists(path_pattern):
        print("Arquivo encontrado.")
    else:
        print("Arquivo não encontrado.")

check_file_task = PythonOperator(
    task_id='check_file',
    python_callable=check_file_exists,
    op_kwargs={'path_pattern': 'airflow-astro-python-sdk-workshop/dags/data/user/user_2023_2_28_23_30_28.json'},
    dag=dag,
)
