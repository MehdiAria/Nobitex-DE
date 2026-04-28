from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import boto3
import json
import pandas as pd
import psycopg2

# ==============================================================================
# 🎓 Data Engineering Tutorial: DAG Definition & Connections
# آموزش مهندسی داده: تعریف DAG و اتصالات
#
# A DAG (Directed Acyclic Graph) is a collection of all the tasks you want to run,
# organized in a way that reflects their relationships and dependencies.
# یک DAG در Airflow مجموعه‌ای از وظایف است که وابستگی‌های آنها را مشخص می‌کند.
# ==============================================================================

# ----------------- Connection Settings (تنظیمات اتصال) -----------------
def get_s3_client():
    # MinIO is an S3-compatible object storage server. We use it as our Data Lake.
    # مینیو (MinIO) یک فضای ذخیره‌سازی اشیاء مانند آمازون S3 است. ما از آن به عنوان Data Lake استفاده می‌کنیم.
    return boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='admin',
        aws_secret_access_key='password123',
        region_name='us-east-1'
    )

def get_postgres_connection():
    # PostgreSQL acts as our Data Warehouse for structured data.
    # پایگاه داده PostgreSQL به عنوان انبار داده (Data Warehouse) ما برای داده‌های ساختاریافته عمل می‌کند.
    return psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow"
    )

# ==============================================================================
# 🥉 BRONZE LAYER: Raw Data Ingestion
# لایه برنز: دریافت داده‌های خام
#
# In the medallion architecture, the Bronze layer contains raw, unprocessed data.
# We fetch data from Nobitex API and store it exactly as we received it in MinIO.
# در معماری مدالیون، لایه برنز شامل داده‌های خام است. ما داده‌ها را از صرافی نوبیتکس دریافت کرده
# و دقیقاً به همان شکل در Data Lake ذخیره می‌کنیم.
# ==============================================================================
def fetch_and_store_nobitex_data(**kwargs):
    s3_client = get_s3_client()

    # 1. Extract: Fetch live orderbook data
    # ۱. استخراج: دریافت داده‌های زنده لیست سفارشات

    # Create the bucket if it does not exist
    # ساختن باکت (سطل) در صورت عدم وجود
    try:
        s3_client.head_bucket(Bucket='crypto-raw-data')
    except Exception as e:
        print('Bucket does not exist. Creating it...')
        s3_client.create_bucket(Bucket='crypto-raw-data')

    url = "https://apiv2.nobitex.ir/v3/orderbook/BTCIRT"
    response = requests.get(url)
    raw_data = response.json()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"nobitex_btc_irt_{timestamp}.json"

    # 2. Load: Save to MinIO bucket
    # ۲. بارگذاری: ذخیره در سطل MinIO
    s3_client.put_object(
        Bucket='crypto-raw-data',
        Key=file_name,
        Body=json.dumps(raw_data).encode('utf-8'),
        ContentType='application/json'
    )
    print(f"✅ Data saved to Data Lake as: {file_name}")

    # 3. XCom (Cross-Task Communication): Return the filename so the next task can use it
    # ۳. ارتباط بین تسک‌ها (XCom): نام فایل را برمی‌گردانیم تا تسک بعدی بتواند از آن استفاده کند
    return file_name


# ==============================================================================
# 🥈 SILVER LAYER: Data Processing & Cleaning
# لایه نقره‌ای: پردازش و پاک‌سازی داده‌ها
#
# The Silver layer contains filtered, cleaned, and augmented data.
# We read the raw JSON from Bronze, process it with Pandas, and load it into Postgres.
# لایه نقره‌ای شامل داده‌های تمیز شده و پردازش شده است. داده‌های خام را می‌خوانیم،
# با کتابخانه قدرتمند Pandas پردازش می‌کنیم و در دیتابیس رابطه‌ای ذخیره می‌کنیم.
# ==============================================================================
def process_orderbook_to_silver(**kwargs):
    # 1. Get filename from XCom (the return value of the previous task)
    # ۱. گرفتن نام فایل از XCom (مقدار بازگشتی تسک قبلی)
    ti = kwargs['ti']
    file_name = ti.xcom_pull(task_ids='ingest_to_minio')

    # 2. Read raw data from MinIO
    # ۲. خواندن داده‌های خام از MinIO
    s3_client = get_s3_client()
    response = s3_client.get_object(Bucket='crypto-raw-data', Key=file_name)
    raw_content = response['Body'].read().decode('utf-8')
    data = json.loads(raw_content)

    # 3. Powerful Processing with PANDAS (Transform)
    # ۳. پردازش قدرتمند با PANDAS (تبدیل داده)

    # Create DataFrames for buyers (bids) and sellers (asks)
    # ساختن دیتافریم (جدول) برای خریداران و فروشندگان
    df_bids = pd.DataFrame(data.get('bids', []), columns=['price', 'volume'])
    df_asks = pd.DataFrame(data.get('asks', []), columns=['price', 'volume'])

    # Convert string prices to numeric for calculations
    # تبدیل ستون قیمت از متن به عدد برای محاسبه
    df_bids['price'] = pd.to_numeric(df_bids['price'], errors='coerce')
    df_asks['price'] = pd.to_numeric(df_asks['price'], errors='coerce')

    # Fast extraction of best prices using Pandas
    # استخراج مقادیر با سرعت بالای پانداس
    best_bid = float(df_bids['price'].max()) if not df_bids.empty else 0
    best_ask = float(df_asks['price'].min()) if not df_asks.empty else 0
    spread = best_ask - best_bid # Calculate the bid-ask spread

    print(f"📊 Pandas Processing Done! Spread: {spread}")

    # 4. Save to PostgreSQL Silver Layer (Load)
    # ۴. ذخیره در PostgreSQL (لایه نقره‌ای)
    conn = get_postgres_connection()
    cursor = conn.cursor()

    # Create table if it doesn't exist
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

    # Insert data into the table
    # ورود داده‌ها به جدول
    cursor.execute("""
        INSERT INTO silver_orderbook (timestamp, best_bid, best_ask, spread)
        VALUES (%s, %s, %s, %s)
    """, (data.get('lastUpdate'), best_bid, best_ask, spread))

    conn.commit()
    cursor.close()
    conn.close()

    print(f"💽 Successfully saved to PostgreSQL Silver Layer!")


# ==============================================================================
# ⏱️ DAG & Pipeline Configuration
# تنظیمات پایپ‌لاین
# ==============================================================================
default_args = {
    'owner': 'mehdi_shahidi',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='nobitex_market_data_pipeline',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='*/5 * * * *', # Run every 5 minutes (اجرا هر ۵ دقیقه)
    catchup=False # Don't backfill past missing runs (عدم اجرای تسک‌های گذشته)
) as dag:

    # Task 1: Ingest (Bronze)
    ingest_task = PythonOperator(
        task_id='ingest_to_minio',
        python_callable=fetch_and_store_nobitex_data,
        provide_context=True
    )

    # Task 2: Process (Silver)
    process_task = PythonOperator(
        task_id='process_to_silver',
        python_callable=process_orderbook_to_silver,
        provide_context=True
    )

    # 🔗 Define Task Dependencies
    # تعیین ترتیب اجرا: اول دریافت داده، سپس پردازش آن
    ingest_task >> process_task
