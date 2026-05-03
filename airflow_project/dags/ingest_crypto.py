# =============================================================================
# پایپ‌لاین داده نوبیتکس — معماری مدالیون (Bronze → Silver → Gold)
# =============================================================================
#
# این فایل قلب پروژه است. یک DAG تعریف می‌کند که Airflow آن را هر ۵ دقیقه اجرا می‌کند.
#
# ─────────────────────────────────────────────────────────────────────────────
# DAG چیست؟ (برای کسی که از Django می‌آید)
# ─────────────────────────────────────────────────────────────────────────────
#   در Django، یک View یک درخواست HTTP را پردازش می‌کند.
#   در Airflow، یک DAG یک "وظیفه زمان‌بندی‌شده" را تعریف می‌کند.
#
#   DAG مخفف Directed Acyclic Graph (گراف جهت‌دار بدون حلقه) است.
#   ساده‌اش: یک لیست از وظایف (Tasks) که ترتیب اجرای آنها مشخص است.
#
#   مثال این پروژه:
#     Task 1: دریافت داده از API نوبیتکس و ذخیره در MinIO  ← Bronze
#     Task 2: خواندن از MinIO، پردازش با Pandas، ذخیره در PostgreSQL ← Silver
#     Task 3: محاسبه آمار ساعتی و ذخیره در جدول Gold  ← Gold
#
#   Task 1 باید قبل از Task 2 تمام شود.
#   Task 2 باید قبل از Task 3 تمام شود.
#   این ترتیب با علامت >> نوشته می‌شود: task1 >> task2 >> task3
#
# ─────────────────────────────────────────────────────────────────────────────
# معماری مدالیون (Medallion Architecture) چیست؟
# ─────────────────────────────────────────────────────────────────────────────
#   یک استاندارد صنعتی برای سازمان‌دهی داده‌ها در سه لایه:
#
#   🥉 Bronze (برنز) = داده خام، دست‌نخورده، دقیقاً همان‌طور که از منبع آمده
#                      اینجا: فایل JSON در MinIO
#
#   🥈 Silver (نقره) = داده تمیز شده، پردازش شده، ساختارمند
#                      اینجا: جدول silver_orderbook در PostgreSQL
#
#   🥇 Gold (طلا)   = داده تجمیع‌شده، آماده برای گزارش و داشبورد
#                      اینجا: جدول gold_hourly_stats در PostgreSQL
#
# ─────────────────────────────────────────────────────────────────────────────
# MinIO چیست؟
# ─────────────────────────────────────────────────────────────────────────────
#   MinIO یک سرور ذخیره‌سازی فایل است که دقیقاً مثل Amazon S3 رفتار می‌کند.
#   ما از آن به عنوان "Data Lake" استفاده می‌کنیم — جایی که داده‌های خام
#   را به‌صورت فایل ذخیره می‌کنیم.
#
#   Data Lake vs Database:
#     Data Lake (MinIO) → فایل‌های خام (JSON, CSV, Parquet) — بدون ساختار
#     Database (PostgreSQL) → جداول ساختارمند با ستون و ردیف
#
# =============================================================================

# ── وارد کردن کتابخانه‌ها ────────────────────────────────────────────────────

from airflow import DAG                          # کلاس اصلی برای تعریف DAG
from airflow.operators.python import PythonOperator  # اجرای یک تابع پایتون به عنوان Task
from datetime import datetime, timedelta         # کار با تاریخ و زمان

import requests    # ارسال درخواست HTTP به API نوبیتکس (مثل fetch در JavaScript)
import boto3       # کتابخانه آمازون برای کار با S3/MinIO
import json        # تبدیل Python dict به رشته JSON و برعکس
import pandas as pd  # کتابخانه قدرتمند برای پردازش داده (جدول‌ها)
import psycopg2    # اتصال مستقیم به PostgreSQL (بدون ORM، خود SQL)

from botocore.client import Config  # تنظیمات پیشرفته برای boto3


# =============================================================================
# توابع کمکی: اتصال به سرویس‌ها
# =============================================================================

def get_s3_client():
    """
    یک کلاینت برای ارتباط با MinIO (Data Lake ما) می‌سازد.

    boto3 کتابخانه رسمی AWS است ولی از آنجا که MinIO کاملاً با پروتکل S3 سازگار است،
    همین کتابخانه را برای MinIO هم استفاده می‌کنیم.

    نکته مهم — addressing_style='path':
      boto3 به‌صورت پیش‌فرض آدرس‌های "virtual-hosted" می‌سازد:
        http://crypto-raw-data.minio:9000/filename.json
      اما MinIO داخل Docker نمی‌تواند این آدرس را resolve کند!

      با تنظیم addressing_style='path'، آدرس به این شکل می‌شود:
        http://minio:9000/crypto-raw-data/filename.json
      که داخل Docker کار می‌کند.
    """
    return boto3.client(
        's3',
        endpoint_url='http://minio:9000',     # آدرس MinIO داخل Docker
        aws_access_key_id='admin',            # نام کاربری MinIO (در docker-compose تعریف شده)
        aws_secret_access_key='password123',  # رمز عبور MinIO
        region_name='us-east-1',             # AWS region — برای MinIO اهمیتی ندارد ولی باید باشد
        config=Config(
            signature_version='s3v4',              # نسخه امضای درخواست
            s3={'addressing_style': 'path'}        # الزامی برای MinIO
        )
    )


def get_postgres_connection():
    """
    یک اتصال مستقیم به PostgreSQL می‌سازد.

    در Django از ORM استفاده می‌کردیم و هرگز مستقیم با psycopg2 کار نمی‌کردیم.
    اینجا چون در Airflow هستیم (نه Django)، مستقیم با psycopg2 کار می‌کنیم.

    نام هاست چرا 'postgres' است نه 'localhost'؟
      چون Airflow داخل Docker اجرا می‌شود.
      داخل Docker، سرویس‌ها با نام‌شان در docker-compose.yml به هم دسترسی دارند.
      localhost در داخل Docker به همان container اشاره می‌کند، نه به postgres!
    """
    return psycopg2.connect(
        host="postgres",      # نام سرویس در docker-compose.yml
        database="airflow",   # نام دیتابیس
        user="airflow",       # نام کاربری PostgreSQL
        password="airflow",   # رمز عبور PostgreSQL
        connect_timeout=10,   # اگر در ۱۰ ثانیه وصل نشد، خطا بده
    )


# =============================================================================
# 🥉 BRONZE LAYER: دریافت داده خام از API و ذخیره در MinIO
# =============================================================================
#
# این تابع سه کار می‌کند:
#   1. EXTRACT   → داده را از API نوبیتکس دریافت می‌کند
#   2. (No Transform) → داده را دست‌نخورده نگه می‌دارد
#   3. LOAD      → داده را در MinIO (Data Lake) ذخیره می‌کند
#
# این الگو را ETL یا EL می‌نامند (Extract, Transform, Load)
#
def fetch_and_store_nobitex_data(**kwargs):
    """
    داده زنده orderbook بیت‌کوین را از API نوبیتکس دریافت کرده
    و به‌صورت فایل JSON در MinIO ذخیره می‌کند.

    **kwargs چیست؟
      Airflow اطلاعاتی مثل زمان اجرا، ID تسک و... را به تابع می‌فرستد.
      با **kwargs می‌گوییم: "هر چیزی که Airflow فرستاد، قبول کن."
    """
    s3 = get_s3_client()
    bucket = 'crypto-raw-data'  # نام سطل (Bucket) در MinIO

    # ── ساخت Bucket اگر وجود ندارد ──────────────────────────────────────────
    #
    # Bucket در S3/MinIO مثل یک پوشه اصلی است.
    # head_bucket بررسی می‌کند که آیا bucket وجود دارد.
    # اگر نه، خطا می‌دهد که ما آن را catch می‌کنیم و bucket را می‌سازیم.
    #
    try:
        s3.head_bucket(Bucket=bucket)
        print(f"Bucket '{bucket}' already exists.")
    except Exception:
        s3.create_bucket(Bucket=bucket)
        print(f"Bucket '{bucket}' created.")

    # ── دریافت داده از API نوبیتکس ──────────────────────────────────────────
    #
    # Orderbook (دفتر سفارشات) چیست؟
    #   لیست تمام سفارش‌های خرید (bids) و فروش (asks) فعال در بازار.
    #   bids: خریدارانی که حاضرند با چه قیمتی بخرند
    #   asks: فروشندگانی که حاضرند با چه قیمتی بفروشند
    #
    url = "https://apiv2.nobitex.ir/v3/orderbook/BTCIRT"
    response = requests.get(
        url,
        timeout=15  # اگر API در ۱۵ ثانیه جواب نداد، خطا بده (از hang شدن جلوگیری می‌کند)
    )

    # raise_for_status() → اگر HTTP status code خطا باشد (4xx یا 5xx)، exception پرتاب کن
    # بدون این، حتی اگر API خطا برگرداند، کد ادامه می‌یابد!
    response.raise_for_status()

    raw_data = response.json()  # متن JSON پاسخ را به Python dict تبدیل می‌کند

    # ── اعتبارسنجی پاسخ API ─────────────────────────────────────────────────
    #
    # قبل از ذخیره، بررسی می‌کنیم که پاسخ API ساختار مورد انتظار را دارد.
    # APIs گاهی ساختارشان تغییر می‌کند یا خطا برمی‌گرداند.
    #
    if 'bids' not in raw_data or 'asks' not in raw_data:
        raise ValueError(
            f"API پاسخ غیرمنتظره‌ای داد. کلیدهای موجود: {list(raw_data.keys())}"
        )

    # ── ذخیره در MinIO (Bronze Layer) ───────────────────────────────────────
    #
    # نام فایل شامل timestamp می‌شود تا هر اجرا فایل جداگانه‌ای داشته باشد.
    # مثال: nobitex_btc_irt_20240315_143022.json
    #
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"nobitex_btc_irt_{timestamp}.json"

    s3.put_object(
        Bucket=bucket,
        Key=file_name,                                      # نام "مسیر" فایل در MinIO
        Body=json.dumps(raw_data, ensure_ascii=False).encode('utf-8'),  # محتوای فایل
        ContentType='application/json'                      # نوع محتوا
    )
    print(f"🥉 Bronze: فایل '{file_name}' در MinIO ذخیره شد.")

    # ── XCom: انتقال اطلاعات به تسک بعدی ──────────────────────────────────
    #
    # XCom مخفف Cross-Communication است.
    # وقتی یک تابع در Airflow چیزی return می‌کند، Airflow آن را در دیتابیس ذخیره می‌کند.
    # تسک بعدی می‌تواند این مقدار را با xcom_pull بخواند.
    #
    # اینجا نام فایل را برمی‌گردانیم تا تسک Silver بداند کدام فایل را بخواند.
    #
    return file_name


# =============================================================================
# 🥈 SILVER LAYER: پردازش، تمیزکاری و ذخیره در PostgreSQL
# =============================================================================
#
# این تابع:
#   1. فایل JSON را از MinIO می‌خواند (Extract از Bronze)
#   2. با Pandas پردازش می‌کند (Transform)
#   3. نتیجه را در PostgreSQL ذخیره می‌کند (Load)
#
def process_orderbook_to_silver(**kwargs):
    """
    داده خام JSON را از MinIO خوانده، با Pandas پردازش می‌کند،
    و نتیجه تمیز را در جدول silver_orderbook پایگاه داده ذخیره می‌کند.
    """

    # ── دریافت نام فایل از XCom ─────────────────────────────────────────────
    #
    # ti مخفف "task instance" است — اطلاعات مربوط به این اجرای خاص تسک
    # xcom_pull → مقداری که تسک 'ingest_to_minio' return کرده را می‌خواند
    #
    ti = kwargs['ti']
    file_name = ti.xcom_pull(task_ids='ingest_to_minio')

    if not file_name:
        raise ValueError("XCom خالی برگشت! تسک ingest_to_minio احتمالاً شکست خورده.")

    # ── خواندن فایل از MinIO ────────────────────────────────────────────────
    s3 = get_s3_client()
    obj = s3.get_object(Bucket='crypto-raw-data', Key=file_name)
    raw_content = obj['Body'].read().decode('utf-8')  # bytes را به string تبدیل می‌کند
    data = json.loads(raw_content)  # string JSON را به Python dict تبدیل می‌کند

    # ── پردازش با Pandas ────────────────────────────────────────────────────
    #
    # Pandas چیست؟ (برای کسی که فقط Django می‌داند)
    #   Pandas مثل یک Excel قدرتمند در پایتون است.
    #   DataFrame = یک جدول با ردیف و ستون
    #
    # ساختار داده API:
    #   data['bids'] = [['1000000', '0.5'], ['999000', '1.2'], ...]
    #                     قیمت     حجم
    #
    # هر عنصر یک لیست [قیمت, حجم] است که هر دو به‌صورت string هستند!
    #

    # ساخت DataFrame برای سفارشات خرید (Bids = خریداران)
    df_bids = pd.DataFrame(
        data.get('bids', []),      # لیست سفارشات خرید (اگر نبود، لیست خالی)
        columns=['price', 'volume']  # نام ستون‌ها
    )

    # ساخت DataFrame برای سفارشات فروش (Asks = فروشندگان)
    df_asks = pd.DataFrame(
        data.get('asks', []),
        columns=['price', 'volume']
    )

    # تبدیل ستون price از string به عدد (numeric)
    # errors='coerce' → اگر تبدیل نشد، NaN (مقدار خالی) قرار بده
    df_bids['price'] = pd.to_numeric(df_bids['price'], errors='coerce')
    df_asks['price'] = pd.to_numeric(df_asks['price'], errors='coerce')

    # ── محاسبه معیارهای کلیدی بازار ────────────────────────────────────────
    #
    # Best Bid = بهترین قیمت خرید = بالاترین قیمتی که خریداران حاضرند بپردازند
    # Best Ask = بهترین قیمت فروش = پایین‌ترین قیمتی که فروشندگان قبول می‌کنند
    # Spread   = اختلاف بین Best Ask و Best Bid (نشانه‌ای از نقدینگی بازار)
    #
    best_bid = float(df_bids['price'].max()) if not df_bids.empty else 0.0
    best_ask = float(df_asks['price'].min()) if not df_asks.empty else 0.0
    spread = best_ask - best_bid

    last_update = data.get('lastUpdate', 0)  # timestamp آخرین به‌روزرسانی orderbook

    print(f"🥈 Silver: best_bid={best_bid:,.0f}  best_ask={best_ask:,.0f}  spread={spread:,.0f}")

    # ── ذخیره در PostgreSQL ─────────────────────────────────────────────────
    #
    # در Django، از ORM استفاده می‌کردیم: MyModel.objects.create(...)
    # اینجا مستقیم SQL می‌نویسیم با psycopg2.
    #
    conn = get_postgres_connection()
    cursor = conn.cursor()  # cursor = ابزار اجرای SQL

    # ساخت جدول اگر وجود ندارد (CREATE TABLE IF NOT EXISTS)
    # این دستور اگر جدول وجود داشته باشد، خطا نمی‌دهد — idempotent است
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS silver_orderbook (
            id          SERIAL PRIMARY KEY,
            last_update BIGINT,
            best_bid    NUMERIC,
            best_ask    NUMERIC,
            spread      NUMERIC,
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    #
    # توضیح ستون‌ها:
    #   id          → شناسه یکتای خودکار (مثل AutoField در Django)
    #   last_update → timestamp آخرین به‌روزرسانی orderbook در نوبیتکس
    #   best_bid    → بهترین قیمت خرید (تومان)
    #   best_ask    → بهترین قیمت فروش (تومان)
    #   spread      → اختلاف قیمت (تومان)
    #   created_at  → زمان ثبت رکورد در دیتابیس ما (خودکار)
    #

    # وارد کردن داده
    # %s → placeholder برای جلوگیری از SQL Injection (مثل parameterized query)
    cursor.execute("""
        INSERT INTO silver_orderbook (last_update, best_bid, best_ask, spread)
        VALUES (%s, %s, %s, %s)
    """, (last_update, best_bid, best_ask, spread))

    conn.commit()   # تایید نهایی تغییرات — بدون این، هیچ چیز ذخیره نمی‌شود!
    cursor.close()  # آزادسازی منابع cursor
    conn.close()    # بستن اتصال به دیتابیس

    print("🥈 Silver: رکورد با موفقیت در PostgreSQL ذخیره شد.")


# =============================================================================
# 🥇 GOLD LAYER: تجمیع ساعتی برای تحلیل و داشبورد
# =============================================================================
#
# Gold Layer = داده‌های آماده برای گزارش‌گیری، بی‌نیاز به پردازش بیشتر
#
# این تابع هر بار که اجرا می‌شود، ساعت‌هایی که هنوز در Gold نیستند
# را از Silver می‌خواند و میانگین، کمینه و بیشینه‌شان را محاسبه می‌کند.
#
def aggregate_to_gold(**kwargs):
    """
    داده‌های Silver را به تفکیک ساعت تجمیع کرده و در جدول Gold ذخیره می‌کند.
    """
    conn = get_postgres_connection()
    cursor = conn.cursor()

    # ساخت جدول Gold اگر وجود ندارد
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS gold_hourly_stats (
            id           SERIAL PRIMARY KEY,
            hour_bucket  TIMESTAMP,
            avg_bid      NUMERIC,
            avg_ask      NUMERIC,
            avg_spread   NUMERIC,
            min_spread   NUMERIC,
            max_spread   NUMERIC,
            record_count INTEGER,
            created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    #
    # توضیح ستون‌ها:
    #   hour_bucket  → ابتدای ساعت (مثلاً 2024-03-15 14:00:00)
    #   avg_bid      → میانگین بهترین قیمت خرید در آن ساعت
    #   avg_ask      → میانگین بهترین قیمت فروش در آن ساعت
    #   avg_spread   → میانگین اسپرد در آن ساعت
    #   min_spread   → کمترین اسپرد در آن ساعت
    #   max_spread   → بیشترین اسپرد در آن ساعت
    #   record_count → چند رکورد در آن ساعت وجود داشته (باید ≈ 12 باشد برای هر ۵ دقیقه)
    #

    # ── INSERT INTO ... SELECT ───────────────────────────────────────────────
    #
    # این SQL یک الگوی قدرتمند است:
    #   1. از جدول silver_orderbook بخوان
    #   2. GROUP BY ساعت (date_trunc('hour', created_at))
    #   3. فقط ساعت‌هایی را که هنوز در gold_hourly_stats نیستند پردازش کن
    #   4. نتیجه را مستقیم INSERT کن
    #
    # date_trunc('hour', '2024-03-15 14:37:22') → '2024-03-15 14:00:00'
    # یعنی دقیقه و ثانیه را حذف می‌کند و فقط ساعت را نگه می‌دارد
    #
    cursor.execute("""
        INSERT INTO gold_hourly_stats
            (hour_bucket, avg_bid, avg_ask, avg_spread, min_spread, max_spread, record_count)
        SELECT
            date_trunc('hour', created_at)  AS hour_bucket,
            ROUND(AVG(best_bid),  2)        AS avg_bid,
            ROUND(AVG(best_ask),  2)        AS avg_ask,
            ROUND(AVG(spread),    2)        AS avg_spread,
            ROUND(MIN(spread),    2)        AS min_spread,
            ROUND(MAX(spread),    2)        AS max_spread,
            COUNT(*)                        AS record_count
        FROM silver_orderbook
        WHERE date_trunc('hour', created_at) NOT IN (
            SELECT hour_bucket FROM gold_hourly_stats
        )
        GROUP BY date_trunc('hour', created_at)
    """)

    conn.commit()
    cursor.close()
    conn.close()
    print("🥇 Gold: آمار ساعتی با موفقیت به‌روز شد.")


# =============================================================================
# تعریف DAG و Tasks
# =============================================================================
#
# default_args → تنظیمات پیش‌فرض که به تمام Tasks داخل این DAG اعمال می‌شود
#
default_args = {
    'owner': 'mehdi_shahidi',       # نام صاحب DAG (نمایشی در UI)
    'retries': 1,                   # اگر تسک شکست خورد، یک بار دیگر امتحان کن
    'retry_delay': timedelta(minutes=2),  # بین امتحان اول و دوم ۲ دقیقه صبر کن
}

# ── تعریف DAG با context manager ────────────────────────────────────────────
#
# "with DAG(...) as dag:" یعنی هر PythonOperator داخل این بلاک
# به این DAG تعلق دارد.
#
with DAG(
    dag_id='nobitex_market_data_pipeline',    # شناسه یکتای DAG (در UI نمایش داده می‌شود)
    default_args=default_args,
    description='پایپ‌لاین داده نوبیتکس: Bronze → Silver → Gold',
    start_date=datetime(2024, 1, 1),          # از چه تاریخی شروع به برنامه‌ریزی کند
    schedule_interval='*/5 * * * *',           # هر ۵ دقیقه اجرا شود (فرمت Cron)
    catchup=False,                            # تسک‌های گذشته را اجرا نکن
    tags=['nobitex', 'crypto', 'medallion'],  # برچسب‌ها برای فیلتر در UI
) as dag:

    # ── Task 1: Bronze Layer ─────────────────────────────────────────────────
    #
    # PythonOperator = اجرای یک تابع پایتون
    # python_callable = کدام تابع اجرا شود
    # provide_context = آیا kwargs (شامل ti برای XCom) به تابع پاس داده شود
    #
    ingest_task = PythonOperator(
        task_id='ingest_to_minio',                    # شناسه یکتای این تسک
        python_callable=fetch_and_store_nobitex_data, # تابعی که اجرا می‌شود
        provide_context=True,                          # برای دسترسی به XCom
    )

    # ── Task 2: Silver Layer ─────────────────────────────────────────────────
    silver_task = PythonOperator(
        task_id='process_to_silver',
        python_callable=process_orderbook_to_silver,
        provide_context=True,
    )

    # ── Task 3: Gold Layer ───────────────────────────────────────────────────
    gold_task = PythonOperator(
        task_id='aggregate_to_gold',
        python_callable=aggregate_to_gold,
        provide_context=True,
    )

    # ── تعریف ترتیب اجرا ────────────────────────────────────────────────────
    #
    # >> یعنی "باید قبل از" — مثل dependency در npm یا Django migration
    #
    # ingest_task >> silver_task >> gold_task
    # یعنی:
    #   اول ingest_task اجرا شود
    #   بعد از موفقیت آن، silver_task اجرا شود
    #   بعد از موفقیت آن، gold_task اجرا شود
    #
    ingest_task >> silver_task >> gold_task
