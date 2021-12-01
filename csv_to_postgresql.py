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


'''Load CSV >> PostgreSQL''' 

#default arguments 
default_args = {
    'owner': 'aleksei',
    'depends_on_past': False,    
    'start_date': datetime(2021, 11, 1),
}

#DAG
dag = DAG('insert_data_postgres',
          default_args=default_args,
          schedule_interval='@once',
          catchup=False)

def file_path(relative_path):
    dir = os.path.dirname(os.path.abspath(__file__))
    split_path = relative_path.split("/")
    new_path = os.path.join(dir, *split_path)
    return new_path

def csv_to_postgres():
    #Open Postgres Connection
    pg_hook = PostgresHook(postgres_conn_id='postgres_1')
    get_postgres_conn = PostgresHook(postgres_conn_id='postgres_1').get_conn()
    curr = get_postgres_conn.cursor()
    

    # CSV loading to table
    with open(file_path("transactions.csv"), "r") as f:
        next(f)
        curr.copy_from(f, 'transactions_csv', sep=',')
        get_postgres_conn.commit()

    with open(file_path("users.csv"), "r") as f:
        next(f)
        curr.copy_from(f, 'users_csv', sep=',')
        get_postgres_conn.commit()

    with open(file_path("webinar 2.0.csv"), "r") as f:
        next(f)
        curr.copy_from(f, 'webinar_csv', sep=',')
        get_postgres_conn.commit()

#Tasks 
task1 = PostgresOperator(task_id = 'create_table_1',
                        sql="""
                        CREATE TABLE IF NOT EXISTS transactions_csv (    
                            user_id INTEGER NOT NULL,
                            price INTEGER NOT NULL);
                            """,
                            postgres_conn_id= 'postgres_1', 
                            autocommit=True,
                            dag= dag)

task2 = PostgresOperator(task_id = 'create_table_2',
                        sql="""
                        CREATE TABLE IF NOT EXISTS users_csv (    
                            user_id INTEGER NOT NULL,
                            email VARCHAR(255),
                            date_registration TIMESTAMP NOT NULL);
                            """,
                            postgres_conn_id= 'postgres_1', 
                            autocommit=True,
                            dag= dag)

task3 = PostgresOperator(task_id = 'create_table_3',
                        sql="""
                        CREATE TABLE IF NOT EXISTS webinar_csv (    
                            email VARCHAR(255));
                            """,
                            postgres_conn_id= 'postgres_1', 
                            autocommit=True,
                            dag= dag)                            


task4 = PythonOperator(task_id='csv_to_database',
                   provide_context=False,
                   python_callable=csv_to_postgres,
                   dag=dag)


task5 = PostgresOperator(task_id = 'create_materialized_view',
                        sql="""
                        CREATE MATERIALIZED VIEW IF NOT EXISTS materialized_view AS    
                            SELECT t.user_id as userid, sum(t.price) AS sum_transactions
                                FROM transactions_csv t INNER JOIN (select u.user_id , u.email 
	                            FROM users_csv u INNER JOIN webinar_csv w ON u.email = w.email 
	                            WHERE date_registration >= '2016-04-01') t2 ON t.user_id = t2.user_id
                            GROUP BY userid;
                            """,
                            postgres_conn_id= 'postgres_1', 
                            autocommit=True,
                            dag= dag) 


task1 >> task2 >> task3 >> task4 >> task5