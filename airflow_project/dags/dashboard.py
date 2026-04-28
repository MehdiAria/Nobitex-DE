import streamlit as st
import pandas as pd
import psycopg2

st.set_page_config(page_title="Crypto Market Dashboard", layout="wide")
st.title("📈 داشبورد زنده بازار بیت‌کوین (نوبیتکس)")

@st.cache_resource
def init_connection():
    return psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow"
    )

conn = init_connection()

query = "SELECT created_at, best_bid, best_ask, spread FROM silver_orderbook ORDER BY created_at DESC LIMIT 100;"

# استفاده از Try-Except برای جلوگیری از کرش کردن در صورت نبود جدول
try:
    df = pd.read_sql(query, conn)

    if not df.empty:
        col1, col2, col3 = st.columns(3)
        latest = df.iloc[0]

        col1.metric("بهترین خریدار (تومان)", f"{latest['best_bid']:,.0f}")
        col2.metric("بهترین فروشنده (تومان)", f"{latest['best_ask']:,.0f}")
        col3.metric("اسپرد فعلی (تومان)", f"{latest['spread']:,.0f}")

        st.markdown("---")

        st.subheader("روند تغییرات اختلاف قیمت (Spread)")
        chart_data = df[['created_at', 'spread']].set_index('created_at')
        st.line_chart(chart_data)
    else:
        st.warning("هنوز دیتایی در دیتابیس وجود ندارد. منتظر اجرای ایرفلو بمانید...")

except Exception as e:
    st.warning("⏳ دیتابیس یا جدول هنوز توسط Airflow ساخته نشده است. لطفا بررسی کنید که DAG در ایرفلو اجرا شده باشد.")
