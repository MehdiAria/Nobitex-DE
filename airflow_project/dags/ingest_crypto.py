from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import boto3
import json
import pandas as pd
import psycopg2
from botocore.client import Config

# ==============================================================================
# Connection helpers
# ==============================================================================

def get_s3_client():
    """
    Connect to MinIO (our Data Lake) using the boto3 S3-compatible client.
    MinIO requires path-style addressing, which we enable via the Config object.
    Without addressing_style='path', boto3 tries virtual-hosted style URLs
    (bucket.minio:9000) which don't resolve inside Docker.
    """
    return boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='admin',
        aws_secret_access_key='password123',
        region_name='us-east-1',
        config=Config(
            signature_version='s3v4',
            s3={'addressing_style': 'path'}
        )
    )


def get_postgres_connection():
    return psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow",
        connect_timeout=10
    )


# ==============================================================================
# BRONZE LAYER: Raw Data Ingestion
# لایه برنز: دریافت داده‌های خام از API نوبیتکس و ذخیره در Data Lake (MinIO)
#
# Medallion Architecture – Bronze = raw, unmodified data exactly as received.
# ==============================================================================

def fetch_and_store_nobitex_data(**kwargs):
    s3 = get_s3_client()
    bucket = 'crypto-raw-data'

    # Create bucket if it does not exist yet
    try:
        s3.head_bucket(Bucket=bucket)
    except Exception:
        s3.create_bucket(Bucket=bucket)
        print(f"Bucket '{bucket}' created.")

    # Fetch live BTC/IRT orderbook from Nobitex
    url = "https://apiv2.nobitex.ir/v3/orderbook/BTCIRT"
    response = requests.get(url, timeout=15)
    response.raise_for_status()
    raw_data = response.json()

    # Validate that the response contains the expected fields
    if 'bids' not in raw_data or 'asks' not in raw_data:
        raise ValueError(f"Unexpected API response structure: {list(raw_data.keys())}")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"nobitex_btc_irt_{timestamp}.json"

    s3.put_object(
        Bucket=bucket,
        Key=file_name,
        Body=json.dumps(raw_data, ensure_ascii=False).encode('utf-8'),
        ContentType='application/json'
    )
    print(f"Bronze layer: saved {file_name} to MinIO bucket '{bucket}'")

    # Return the filename via XCom so the next task can read the same file
    return file_name


# ==============================================================================
# SILVER LAYER: Clean & Transform
# لایه نقره‌ای: تمیزسازی و تبدیل داده با Pandas، سپس ذخیره در PostgreSQL
#
# Medallion Architecture – Silver = cleaned, enriched, structured data.
# ==============================================================================

def process_orderbook_to_silver(**kwargs):
    ti = kwargs['ti']
    file_name = ti.xcom_pull(task_ids='ingest_to_minio')

    if not file_name:
        raise ValueError("XCom returned empty filename from ingest_to_minio task.")

    # Read raw JSON from MinIO (Bronze layer)
    s3 = get_s3_client()
    obj = s3.get_object(Bucket='crypto-raw-data', Key=file_name)
    raw_content = obj['Body'].read().decode('utf-8')
    data = json.loads(raw_content)

    # Transform with Pandas
    # Each bid/ask entry is [price, volume] in string format
    df_bids = pd.DataFrame(data.get('bids', []), columns=['price', 'volume'])
    df_asks = pd.DataFrame(data.get('asks', []), columns=['price', 'volume'])

    df_bids['price'] = pd.to_numeric(df_bids['price'], errors='coerce')
    df_asks['price'] = pd.to_numeric(df_asks['price'], errors='coerce')

    # Best bid = highest price a buyer is willing to pay
    # Best ask = lowest price a seller is willing to accept
    best_bid = float(df_bids['price'].max()) if not df_bids.empty else 0.0
    best_ask = float(df_asks['price'].min()) if not df_asks.empty else 0.0
    spread = best_ask - best_bid

    last_update = data.get('lastUpdate', 0)
    print(f"Silver layer: best_bid={best_bid:,.0f}  best_ask={best_ask:,.0f}  spread={spread:,.0f}")

    # Write to PostgreSQL Silver layer
    conn = get_postgres_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS silver_orderbook (
            id         SERIAL PRIMARY KEY,
            last_update BIGINT,
            best_bid   NUMERIC,
            best_ask   NUMERIC,
            spread     NUMERIC,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cursor.execute("""
        INSERT INTO silver_orderbook (last_update, best_bid, best_ask, spread)
        VALUES (%s, %s, %s, %s)
    """, (last_update, best_bid, best_ask, spread))

    conn.commit()
    cursor.close()
    conn.close()
    print("Silver layer: record inserted into PostgreSQL.")


# ==============================================================================
# GOLD LAYER: Aggregated Analytics
# لایه طلایی: داده‌های تجمیع‌شده برای تحلیل و داشبورد
#
# Medallion Architecture – Gold = business-level aggregates ready for reporting.
# ==============================================================================

def aggregate_to_gold(**kwargs):
    conn = get_postgres_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS gold_hourly_stats (
            id          SERIAL PRIMARY KEY,
            hour_bucket TIMESTAMP,
            avg_bid     NUMERIC,
            avg_ask     NUMERIC,
            avg_spread  NUMERIC,
            min_spread  NUMERIC,
            max_spread  NUMERIC,
            record_count INTEGER,
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Compute per-hour aggregates for hours not yet in the gold table
    cursor.execute("""
        INSERT INTO gold_hourly_stats
            (hour_bucket, avg_bid, avg_ask, avg_spread, min_spread, max_spread, record_count)
        SELECT
            date_trunc('hour', created_at) AS hour_bucket,
            ROUND(AVG(best_bid),  2),
            ROUND(AVG(best_ask),  2),
            ROUND(AVG(spread),    2),
            ROUND(MIN(spread),    2),
            ROUND(MAX(spread),    2),
            COUNT(*)
        FROM silver_orderbook
        WHERE date_trunc('hour', created_at) NOT IN (
            SELECT hour_bucket FROM gold_hourly_stats
        )
        GROUP BY date_trunc('hour', created_at)
    """)

    conn.commit()
    cursor.close()
    conn.close()
    print("Gold layer: hourly aggregates updated.")


# ==============================================================================
# DAG Definition
# ==============================================================================

default_args = {
    'owner': 'mehdi_shahidi',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='nobitex_market_data_pipeline',
    default_args=default_args,
    description='Nobitex BTC/IRT orderbook pipeline: Bronze → Silver → Gold',
    start_date=datetime(2024, 1, 1),
    schedule_interval='*/5 * * * *',
    catchup=False,
    tags=['nobitex', 'crypto', 'medallion'],
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_to_minio',
        python_callable=fetch_and_store_nobitex_data,
        provide_context=True,
    )

    silver_task = PythonOperator(
        task_id='process_to_silver',
        python_callable=process_orderbook_to_silver,
        provide_context=True,
    )

    gold_task = PythonOperator(
        task_id='aggregate_to_gold',
        python_callable=aggregate_to_gold,
        provide_context=True,
    )

    # Bronze → Silver → Gold
    ingest_task >> silver_task >> gold_task
