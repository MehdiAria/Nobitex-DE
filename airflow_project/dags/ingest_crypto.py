from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import boto3
import json
import pandas as pd
import psycopg2

# ----------------- تنظیمات اتصال -----------------
def get_s3_client():
    return boto3.client(
        's3',
        endpoint_url='http://host.docker.internal:9000',
        aws_access_key_id='admin',
        aws_secret_access_key='password123',
        region_name='us-east-1'
    )

def get_postgres_connection():
    return psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow"
    )

# ----------------- تسک اول (لایه برنز) -----------------
def fetch_and_store_nobitex_data(**kwargs):
    s3_client = get_s3_client()

    url = "https://apiv2.nobitex.ir/v3/orderbook/BTCIRT"
    response = requests.get(url)
    raw_data = response.json()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"nobitex_btc_irt_{timestamp}.json"

    s3_client.put_object(
        Bucket='crypto-raw-data',
        Key=file_name,
        Body=json.dumps(raw_data).encode('utf-8'),
        ContentType='application/json'
    )
    print(f"✅ Data saved to Data Lake as: {file_name}")
    return file_name


# ----------------- تسک دوم (لایه نقره‌ای با Pandas) -----------------
def process_orderbook_to_silver(**kwargs):
    # ۱. گرفتن نام فایل از XCom
    ti = kwargs['ti']
    file_name = ti.xcom_pull(task_ids='ingest_to_minio')

    # ۲. خواندن داده‌های خام از MinIO
    s3_client = get_s3_client()
    response = s3_client.get_object(Bucket='crypto-raw-data', Key=file_name)
    raw_content = response['Body'].read().decode('utf-8')
    data = json.loads(raw_content)

    # ۳. پردازش قدرتمند با PANDAS
    # ساختن دیتافریم (جدول) برای خریداران و فروشندگان
    df_bids = pd.DataFrame(data.get('bids', []), columns=['price', 'volume'])
    df_asks = pd.DataFrame(data.get('asks', []), columns=['price', 'volume'])

    # تبدیل ستون قیمت از متن (String) به عدد (Numeric) برای محاسبه
    df_bids['price'] = pd.to_numeric(df_bids['price'], errors='coerce')
    df_asks['price'] = pd.to_numeric(df_asks['price'], errors='coerce')

    # استخراج مقادیر با سرعت بالای پانداس
    best_bid = float(df_bids['price'].max()) if not df_bids.empty else 0
    best_ask = float(df_asks['price'].min()) if not df_asks.empty else 0
    spread = best_ask - best_bid

    print(f"📊 Pandas Processing Done! Spread: {spread}")

    # ۴. ذخیره در PostgreSQL (لایه نقره‌ای)
    conn = get_postgres_connection()
    cursor = conn.cursor()

    # ساخت جدول (اگر وجود نداشت)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS silver_orderbook (
            id SERIAL PRIMARY KEY,
            timestamp BIGINT,
            best_bid NUMERIC,
            best_ask NUMERIC,
            spread NUMERIC,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # ورود داده‌ها به جدول
    cursor.execute("""
        INSERT INTO silver_orderbook (timestamp, best_bid, best_ask, spread)
        VALUES (%s, %s, %s, %s)
    """, (data.get('lastUpdate'), best_bid, best_ask, spread))

    conn.commit()
    cursor.close()
    conn.close()

    print(f"💽 Successfully saved to PostgreSQL Silver Layer!")


# ----------------- تنظیمات پایپ‌لاین -----------------
default_args = {
    'owner': 'mehdi_shahidi',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='nobitex_market_data_pipeline',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='*/5 * * * *',
    catchup=False
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_to_minio',
        python_callable=fetch_and_store_nobitex_data,
        provide_context=True
    )

    process_task = PythonOperator(
        task_id='process_to_silver',
        python_callable=process_orderbook_to_silver,
        provide_context=True
    )

    # تعیین ترتیب اجرا
    ingest_task >> process_task
