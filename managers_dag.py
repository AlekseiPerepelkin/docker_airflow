import numpy as np
from numpy import dtype
import airflow
import os
import psycopg2
import pandas as pd
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
import csv



'''Load CSV >> PostgreSQL''' 

#default arguments 
default_args = {
    'owner': 'aleksei',
    'depends_on_past': False,    
    'start_date': datetime(2021, 12, 28),
}

#DAG
dag = DAG('insert_managers_to_postgres',
          default_args=default_args,
          schedule_interval='0 19 * * 1',
          catchup=False)

def file_path(relative_path):
    dir = os.path.dirname(os.path.abspath(__file__))
    split_path = relative_path.split("/")
    new_path = os.path.join(dir, *split_path)
    return new_path

def _replace(lst): 
    return [i.replace(",", "") for i in lst]

def csv_to_postgres():
    #Open Postgres Connection
    pg_hook = PostgresHook(postgres_conn_id='postgres_1')
    get_postgres_conn = PostgresHook(postgres_conn_id='postgres_1').get_conn()
    curr = get_postgres_conn.cursor()
 
    with open(file_path('/data/managers.csv'), 'r') as f:
        reader = csv.reader(f)
        next(reader) # Skip the header row.
        for row in reader:
            row = _replace(row)
            curr.execute(
            "INSERT INTO DWH.managers VALUES (%s, %s, %s)",
            row
        )
        get_postgres_conn.commit()



task1 = PostgresOperator(task_id = 'create_table',
                        sql="""
                        CREATE SCHEMA IF NOT EXISTS DWH;
                        CREATE TABLE IF NOT EXISTS DWH.managers (    
                                manager VARCHAR(255),
                                work_group VARCHAR(255),
                                qty INTEGER);
                                """,
                                postgres_conn_id= 'postgres_1', 
                                autocommit=True,
                                dag= dag)                            

task2 = PythonOperator(task_id='csv_to_database',
                   provide_context=False,
                   python_callable=csv_to_postgres,
                   dag=dag)

task3 = PostgresOperator(task_id = 'create_materialized_view',
                        sql="""
                            CREATE MATERIALIZED VIEW IF NOT EXISTS dwh.sales_report AS
                                SELECT s.customer, s.customer_type, s.manager, m.work_group, s.jan, s.feb, s.mar, s.apr,
                                        (s.jan + s.feb + s.mar + s.apr) AS summ, m.qty AS manager_qty 
                                    FROM dwh.sales s join dwh.managers m on s.manager = m.manager 
                            """,
                            postgres_conn_id= 'postgres_1', 
                            autocommit=True,
                            dag= dag) 
task1 >> task2 >> task3
