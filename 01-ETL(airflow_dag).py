from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
import pymysql

default_args = {'owner': 'airflow'}

path = "/home/ralmeida/jupyter/academia_pompeia"
path_temp_csv = path+"/pompeia.csv"


dag = DAG(
    dag_id='01-pipeline_academia',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=days_ago(1),
)


def _extract():
    con = mysql.connector.connect(host='192.168.15.8',database='academia',user='user',password='user!123')
    cursor = con.cursor()
    df = pd.read_sql("SELECT Código,\
                 Nascimento,\
                 Sexo,\
                 Consultor,\
                 Professor,\
                 `Último Status`,\
                 Modalidade,\
                 `Como Conheceu`\
                 FROM pompeia\
                 WHERE NOT EXISTS(SELECT * FROM DW where pompeia.Código = DW.`Código de cliente`)", con);
    cursor.close()
    con.close()
    df.to_csv(path_temp_csv, index=False)


def _transform():
    df = pd.read_csv(path_temp_csv, sep = ',')
    df = df.rename({'Código':'Código de cliente'}, axis = 'columns')
    df[['data última presença', 'hora última presença']] = df["Último Status"].str.split(' ', expand = True)
    df.drop(columns =["Último Status"], inplace = True)
    df['Nascimento'] = pd.to_datetime(df['Nascimento'], format="%d/%m/%Y")
    df['data última presença'] = pd.to_datetime(df['data última presença'], format="%d/%m/%Y")
    df['hora última presença'] = pd.to_datetime(df['hora última presença'], format="%H:%M")
    df['Professor'].replace({'-':'SEM PROFESSOR'}, inplace= True)
    correcao = {'FUNCIONAL + MUSCULAÇÃO, NATAÇÃO':'FUNCIONAL + MUSCULAÇÃO + NATAÇÃO','-':'PLANO COMPLETO'}
    df['Modalidade'].replace(correcao, inplace= True)
    df.to_csv(path_temp_csv, index=False)


def _load():
    df = pd.read_csv(path_temp_csv, sep = ',')
    sqlEngine       = create_engine('mysql+pymysql://user:user!123@192.168.15.8:3306/academia', pool_recycle=3600)
    dbConnection    = sqlEngine.connect()
    df.to_sql('dw', dbConnection, index=False, if_exists='append')
    dbConnection.close()


extract_task = PythonOperator(
    task_id="Extract", python_callable=_extract, dag=dag
)

transform_task = PythonOperator(
    task_id="transform", python_callable=_transform, dag=dag
)

load_task = PythonOperator(
    task_id="load", python_callable=_load, dag=dag
)

extract_task >> transform_task >> load_task
