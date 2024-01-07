from pars import get_review
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import datetime
import sqlalchemy


dag = DAG('ETL_test', 
    schedule_interval=datetime.timedelta(days=1),
    start_date=datetime.datetime(2021, 12, 17, 0))

def extract_data():
    # извлекаем отзывы на русском и английском языках
    df = get_review('org.telegram.messenger', 'ru', 'RU')
    df1 = get_review('org.telegram.messenger', 'en', 'EN')
    df = pd.concat([df, df1])
    df.to_csv('extract_data_test_dag.csv', index=False)

def transform_data():
    # обрабатываем полученные оценки
    df = pd.read_csv('extract_data_test_dag.csv')
    df['score'] = df['score'].astype(int)
    grouped = df.groupby(["event_date", "language"])
    df_grouped = grouped.agg(["count", "mean", "max", "min"])
    # удаляем верхний уровень названий
    df_grouped.columns = df_grouped.columns.droplevel()
    # выносим индексы в столбцы
    df_grouped.reset_index(inplace=True)
    df_grouped = df_grouped.rename(columns={
        'count': 'reviews_count',
        'min': 'min_score',
        'mean': 'avg_score',
        'max': 'max_score'
        }
        )
    df_grouped = df_grouped[['event_date', 'language', 'reviews_count', 'min_score', 'avg_score', 'max_score']]
    df_grouped['insert_date'] = datetime.date.today()
    df_grouped['insert_datetime'] = datetime.datetime.now()
    df_grouped.to_csv('transform_data_test_dag.csv', index=False)

def load_data(df):
    # загружаем данные по отзывам в clickhouse
    df = pd.read_csv('transform_data_test_dag.csv')
    host = ''
    # лучше сохранить в переменных средах 
    user_name = ''
    psw = ''
    con = sqlalchemy.create_engine(host.format(user_name, psw))
    df.to_sql(name='public_reviews', con=con, schema='raw_data', if_exists='append', index=False)


task_extract_data = PythonOperator( 
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag
)

task_transform_data = PythonOperator( 
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)

task_load_data = PythonOperator( 
    task_id='load_data',
    python_callable=load_data,
    dag=dag
)

task_extract_data >> task_transform_data >> task_load_data
