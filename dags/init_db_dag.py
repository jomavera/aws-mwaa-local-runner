from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

import datetime
from sqlalchemy import create_engine, text
import pandas as pd

def create_tables():

    # -- # Database connection # -- #

    engine = create_engine(
    "mysql://myuser:mypassword@dw:3306/dw",
    )
    conn = engine.connect()

    stmt_1 = """
    DROP TABLE IF EXISTS device;
    CREATE TABLE IF NOT EXISTS device (
    id	int PRIMARY KEY,
    type int,
    store_id int
    );"""
    
    stmt_2 = """
    DROP TABLE IF EXISTS store;
    CREATE TABLE IF NOT EXISTS store (
            id INT PRIMARY KEY,
            name varchar(100),
            address varchar(255),
            city varchar(50),
            country varchar(50),
            created_at timestamp,
            typology varchar(50),
            customer_id int
    );"""

    stmt_3 = """
    DROP TABLE IF EXISTS transaction;
    CREATE TABLE IF NOT EXISTS transaction
        (
            id INT PRIMARY KEY,
            device_id INT,
            product_name varchar(100),
            product_sku bigint,
            category_name varchar(50),
            amount int,
            status varchar(50),
            card_number bigint,
            cvv int,
            created_at DATETIME,
            happened_at DATETIME
        );
    """

    conn.execute(text(stmt_1))
    conn.execute(text(stmt_2))
    conn.execute(text(stmt_3))

    engine.dispose()

def insert():
    
    engine = create_engine("mysql://myuser:mypassword@dw:3306/dw")

    df_device = pd.read_csv('/usr/local/airflow/device.csv', sep=";")
    df_device.to_sql('device', engine, if_exists='replace', index=False)

    df_store = pd.read_csv('/usr/local/airflow/store.csv', sep=";")
    df_store.to_sql('store', engine, if_exists='replace', index=False)

    df_transaction = pd.read_csv('/usr/local/airflow/transaction.csv', sep=";")
    df_transaction['product_sku'] = df_transaction['product_sku'].map(lambda x: int(float(x.replace('v','')) ))
    df_transaction['card_number'] = df_transaction['card_number'].map(lambda x: int(float(x.replace('v','').replace(' ','')) ))

    df_transaction.to_sql('transaction', engine, if_exists='replace', index=False)

    engine.dispose()

def answer_1():
    engine = create_engine("mysql://myuser:mypassword@dw:3306/dw")
    conn = engine.connect()

    top10_stores_by_amount = \
    """select
        st.id as store_id,
        st.name as store_name,
        sum(trx.amount) as amount
        from transaction as trx
        inner join device as dv on trx.device_id = dv.id
        inner join store as st on dv.store_id = st.id
        group by store_id, store_name
        order by amount desc limit 10;
    """

    query=text(top10_stores_by_amount)
    cursor = conn.execute(query)
    df_1 = pd.DataFrame(cursor.fetchall())

    engine.dispose()
    
    df_1.to_excel('answer_1.xlsx', index=False)

def answer_2():
    engine = create_engine("mysql://myuser:mypassword@dw:3306/dw")
    conn = engine.connect()
    top10_products_sold = \
    """select
        trx.product_sku,
        sum(trx.amount) as amount
        from transaction as trx
        group by trx.product_sku
        order by amount desc limit 10;
    """

    query=text(top10_products_sold)
    cursor = conn.execute(query)
    df_2 = pd.DataFrame(cursor.fetchall())


    engine.dispose()

    df_2.to_excel('answer_2.xlsx', index=False)

def answer_3():
    engine = create_engine("mysql://myuser:mypassword@dw:3306/dw")
    conn = engine.connect()
    avg_trx_amnt_per_typ_country = \
    """select
        st.typology,
        st.country,
        avg(trx.amount) as amount
        from transaction as trx
        inner join device as dv on trx.device_id = dv.id
        inner join store as st on dv.store_id = st.id
        group by st.typology, st.country;
    """
    
    query = text(avg_trx_amnt_per_typ_country)
    cursor = conn.execute(query)
    df_3 = pd.DataFrame(cursor.fetchall())
    df_3['amount'] = df_3['amount'].map(lambda x:round(x,3))
    
    engine.dispose()

    df_3.to_excel('answer_3.xlsx', index=False)

def answer_4():
    engine = create_engine("mysql://myuser:mypassword@dw:3306/dw")
    conn = engine.connect()
    prc_trx_per_device = \
    """select
        dv.type as type,
        count(*) as cnt,
        count(*) * 1.0 / sum(count(*)) over () as percentage
        from transaction as trx
        inner join device as dv on trx.device_id = dv.id
        group by type;
    """
    
    query = text(prc_trx_per_device)
    cursor = conn.execute(query)
    df_4 = pd.DataFrame(cursor.fetchall())
    df_4['percentage'] = df_4['percentage'].map(lambda x: round(x,5))
    df_4 = df_4.drop(columns=['cnt'])
    
    engine.dispose()

    df_4.to_excel('answer_4.xlsx', index=False)

def answer_5():
    engine = create_engine("mysql://myuser:mypassword@dw:3306/dw")
    conn = engine.connect()
    avg_time_5_trx = \
    """
    select
    store_id,
    store_name,
    avg(fifth_trx_time)/60 as avg_hours_to_5_trx
    from
    (select
        id as store_id,
        name as store_name,
        trx_date,
        MAX(TIME_TO_SEC( trx_time ))/60 as fifth_trx_time
    from (
        select
            ROW_NUMBER() OVER (PARTITION BY id, name, trx_date ORDER BY trx_time ASC) AS r,
            sub.*
        from
            (select
                st.id,
                st.name,
                TIME(trx.happened_at) as trx_time,
                DATE(trx.happened_at) as trx_date
                from transaction as trx
                inner join device as dv on trx.device_id = dv.id
                inner join store as st on dv.store_id = st.id) as sub
        ) as x
        where x.r <=5
        group by  id, name, trx_date) as sub2
        group by store_id, store_name;
    """

    query = text(avg_time_5_trx)
    cursor = conn.execute(query)
    df_5 = pd.DataFrame(cursor.fetchall())
    df_5['avg_hours_to_5_trx'] = df_5['avg_hours_to_5_trx'].fillna(0)
    df_5['avg_hours_to_5_trx'] = df_5['avg_hours_to_5_trx'].map(lambda x: round(x, 3))

    engine.dispose()
    df_5.to_excel('answer_5.xlsx', index=False)


dag = DAG(
    'ExtractLoadTransform',
    start_date=datetime.datetime.now(),
)

start_operator = DummyOperator(
  task_id='Begin_execution',
  dag=dag
)

create_task = PythonOperator(
    task_id = 'create_tables',
    python_callable=create_tables,
    dag=dag
)

insert_task = PythonOperator(
    task_id = 'insert_data',
    python_callable=insert,
    dag=dag
)

answer_1_task = PythonOperator(
    task_id = 'answer_question_1',
    python_callable=answer_1,
    dag = dag
)

answer_2_task = PythonOperator(
    task_id = 'answer_question_2',
    python_callable=answer_2,
    dag = dag
)

answer_3_task = PythonOperator(
    task_id = 'answer_question_3',
    python_callable=answer_3,
    dag = dag
)

answer_4_task = PythonOperator(
    task_id = 'answer_question_4',
    python_callable=answer_4,
    dag = dag
)

answer_5_task = PythonOperator(
    task_id = 'answer_question_5',
    python_callable=answer_5,
    dag = dag
)

end_operator = DummyOperator(
  task_id='Stop_execution',
  dag=dag
)

start_operator >> create_task >> insert_task 
insert_task >> answer_1_task  >> end_operator
insert_task >> answer_2_task  >> end_operator
insert_task >> answer_3_task  >> end_operator
insert_task >> answer_4_task  >> end_operator
insert_task >> answer_5_task  >> end_operator