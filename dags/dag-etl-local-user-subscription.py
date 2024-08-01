# import libraries
import os
import pathlib
from datetime import date, datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

import pandas as pd
from pandas import DataFrame

from astro import sql as aql
from astro.files import File
from astro.constants import FileType
from astro.sql.table import Table, Metadata

# connections & variables
POSTGRESS_CONN_ID = "postgres_conn"

# default args & init dag
CWD = pathlib.Path(__file__).parent
default_args = {
    "owner": "claudio souza",
    "retries": 1,
    "retry_delay": 0
}

# declare dag
@dag(
    dag_id="dag-etl-local-user-subscription",
    start_date=datetime(2024, 7, 29),
    max_active_runs=1,
    schedule_interval=timedelta(hours=24),
    default_args=default_args,
    catchup=False,
    tags=['development', 'elt', 'astrosdk']
)
# init main function
def dataframe_etl():

    # init & finish task
    init_data_load = EmptyOperator(task_id="init")
    finish_data_load = EmptyOperator(task_id="finish")

    # load files {user}
    user_file = aql.load_file(
        task_id="user_file",
        input_file=File(path=str(CWD.parent) + "/dags/data/user/user*", filetype=FileType.JSON),
        output_table=Table(name="user", conn_id=POSTGRESS_CONN_ID, metadata=Metadata(schema="astro"),),
        # output_table=Table(metadata=Metadata(schema=schema), name="user", conn_id="postgres_conn"),
        if_exists="replace",
        use_native_support=True,
        columns_names_capitalization="original"     
    )

    # load files {subscription}
    subscription_file = aql.load_file(
        task_id="subscription_file",
        input_file=File(path=str(CWD.parent) + "/dags/data/subscription/subscription*", filetype=FileType.JSON),
        output_table=Table(name="subscription", conn_id=POSTGRESS_CONN_ID, metadata=Metadata(schema="astro"),),
        if_exists="replace",
        use_native_support=True,
        columns_names_capitalization="original"
    )

    # define sequence
    init_data_load >> [user_file, subscription_file] >> finish_data_load


# init dag
dag = dataframe_etl()