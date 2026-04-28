from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import boto3
import json

# ۱. کدهای قبلی خودمان را دقیقاً داخل یک تابع پایتونی قرار می‌دهیم
def fetch_and_store_nobitex_data():
    s3_client = boto3.client(
        's3',
        endpoint_url='http://host.docker.internal:9000', # آدرس داکر
        aws_access_key_id='admin',
        aws_secret_access_key='password123',
        region_name='us-east-1'
    )

    url = "https://apiv2.nobitex.ir/v3/orderbook/BTCIRT"
    response = requests.get(url)
    raw_data = response.json()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"nobitex_btc_usdt_{timestamp}.json"

    s3_client.put_object(
        Bucket='crypto-raw-data',
        Key=file_name,
        Body=json.dumps(raw_data).encode('utf-8'),
        ContentType='application/json'
    )
    print(f"Saved: {file_name}")

# ۲. تنظیمات اولیه پایپ‌لاین (ارکستریشن)
default_args = {
    'owner': 'mehdi_shahidi',
    'retries': 2,                                # اگر API نوبیتکس قطع بود، ۲ بار دیگر تلاش کن
    'retry_delay': timedelta(minutes=1),         # فاصله بین هر تلاش مجدد ۱ دقیقه باشد
}

# ۳. تعریف DAG (نقشه پایپ‌لاین ما)
with DAG(
    dag_id='nobitex_market_data_pipeline',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='*/5 * * * *',             # اجرای خودکار هر ۵ دقیقه (بر اساس منطق Cron)
    catchup=False
) as dag:

    # ۴. تعریف تسک‌ها
    ingest_task = PythonOperator(
        task_id='ingest_to_minio',
        python_callable=fetch_and_store_nobitex_data
    )

    # اگر در آینده تسک دیگری داشتیم (مثل پردازش داده)، اینجا اضافه می‌کنیم
    # مثلاً: ingest_task >> process_task


