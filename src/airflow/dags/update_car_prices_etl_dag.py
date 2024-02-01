from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'hso',
    'start_date': datetime(2024, 2, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}


def read_csv_data(ti) -> None:
    prices_df = pd.read_csv('/opt/airflow/output/stream_result.csv')

    ti.xcom_push(
        key='prices_df',
        value=prices_df
    ) 


def transform_prices(ti) -> None:
    prices_df = ti.xcom_pull(
        task_ids='extract_data_from_csv',
        key='prices_df'
    )
    prices_df.drop(columns=['start', 'end', 'processed_at'], inplace=True)

    prices_df['last_update'] = pd.to_datetime('now')

    transmission_mapping = {'Manual': False, 'Automatic': True}
    prices_df['transmission'] = prices_df['transmission'].map(transmission_mapping)

    prices_df.rename(columns={'average_price': 'price'}, inplace=True)
    prices_df.to_csv('/opt/airflow/output/transformed.csv', index=False)


with DAG(
    dag_id='car_prices_data_pipeline',
    default_args=default_args,
    description='Data pipeline for updating car prices data',
    schedule_interval='*/30 * * * *',  # Adjust the schedule as needed
    render_template_as_native_obj=True
) as dag:


    extract_data_from_csv = PythonOperator(
        task_id='extract_data_from_csv',
        python_callable=read_csv_data,
        dag=dag
    )

    transform_car_prices= PythonOperator(
        task_id='transform_car_prices',
        python_callable=transform_prices,
        dag=dag
    )
    
    create_temp_table_sql = """
        CREATE TEMPORARY TABLE temp_car_prices AS
        SELECT * FROM car_prices WHERE 1 = 0;
    """
    load_csv_to_temp_table_sql="""
        COPY temp_car_prices FROM '/shared/output/transformed.csv' DELIMITER ',' CSV HEADER;
    """

    upsert_sql = """
        INSERT INTO car_prices (brand, model, year, color, mileage, transmission, body_health, engine_health, tires_health, price, last_update)
        SELECT brand, model, year, color, mileage, transmission, body_health, engine_health, tires_health, price, last_update
        FROM temp_car_prices
        ON CONFLICT (brand, model, year, color, mileage, transmission, body_health, engine_health, tires_health)
        DO UPDATE SET price = EXCLUDED.price, last_update = EXCLUDED.last_update;
    """
    upsert_prices = PostgresOperator(
        task_id='upsert_prices',
        sql=[create_temp_table_sql, load_csv_to_temp_table_sql, upsert_sql],
        postgres_conn_id='car_prices_postgres',
        dag=dag,
    )


extract_data_from_csv >> transform_car_prices >> upsert_prices