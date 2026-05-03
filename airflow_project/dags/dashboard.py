import streamlit as st
import pandas as pd
import psycopg2
import time

st.set_page_config(
    page_title="Nobitex Market Dashboard",
    page_icon="📈",
    layout="wide",
)

st.title("📈 داشبورد زنده بازار بیت‌کوین (نوبیتکس)")
st.caption("داده‌ها هر ۳۰ ثانیه به‌روزرسانی می‌شوند | Data refreshes every 30 seconds")


def fetch_data():
    """Open a fresh connection on every call to avoid stale-connection errors."""
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow",
        connect_timeout=5,
    )
    try:
        silver_df = pd.read_sql(
            "SELECT created_at, best_bid, best_ask, spread "
            "FROM silver_orderbook "
            "ORDER BY created_at DESC LIMIT 200",
            conn,
        )
        try:
            gold_df = pd.read_sql(
                "SELECT hour_bucket, avg_bid, avg_ask, avg_spread, "
                "min_spread, max_spread, record_count "
                "FROM gold_hourly_stats "
                "ORDER BY hour_bucket DESC LIMIT 48",
                conn,
            )
        except Exception:
            gold_df = pd.DataFrame()
    finally:
        conn.close()
    return silver_df, gold_df


# ── fetch ────────────────────────────────────────────────────────────────────
try:
    silver_df, gold_df = fetch_data()
except psycopg2.OperationalError as e:
    st.error(f"❌ نمی‌توان به پایگاه داده متصل شد: {e}")
    st.stop()
except Exception as e:
    err = str(e)
    if "silver_orderbook" in err or "does not exist" in err:
        st.warning(
            "⏳ جدول داده هنوز ساخته نشده است.\n\n"
            "مطمئن شوید DAG `nobitex_market_data_pipeline` در Airflow "
            "(http://localhost:8080) فعال است و حداقل یک بار اجرا شده باشد."
        )
    else:
        st.error(f"خطا: {e}")
    st.stop()

# ── Silver layer metrics ──────────────────────────────────────────────────────
if silver_df.empty:
    st.warning("هنوز هیچ داده‌ای در دیتابیس ثبت نشده. منتظر اجرای اول DAG باشید...")
else:
    latest = silver_df.iloc[0]

    col1, col2, col3, col4 = st.columns(4)
    col1.metric(
        "بهترین خریدار (تومان)",
        f"{latest['best_bid']:,.0f}",
    )
    col2.metric(
        "بهترین فروشنده (تومان)",
        f"{latest['best_ask']:,.0f}",
    )
    col3.metric(
        "اسپرد فعلی (تومان)",
        f"{latest['spread']:,.0f}",
    )
    col4.metric(
        "تعداد رکوردهای ثبت‌شده",
        len(silver_df),
    )

    st.markdown("---")

    # Bid / Ask price trend
    st.subheader("روند قیمت خرید و فروش")
    price_chart = (
        silver_df[['created_at', 'best_bid', 'best_ask']]
        .set_index('created_at')
        .sort_index()
    )
    st.line_chart(price_chart)

    # Spread trend
    st.subheader("روند اسپرد (Spread)")
    spread_chart = (
        silver_df[['created_at', 'spread']]
        .set_index('created_at')
        .sort_index()
    )
    st.line_chart(spread_chart)

    # Raw data table
    with st.expander("جدول داده‌های خام (Silver Layer)"):
        st.dataframe(silver_df, use_container_width=True)

# ── Gold layer ────────────────────────────────────────────────────────────────
if not gold_df.empty:
    st.markdown("---")
    st.subheader("📊 آمار ساعتی تجمیع‌شده (Gold Layer)")

    g_col1, g_col2 = st.columns(2)
    with g_col1:
        avg_chart = (
            gold_df[['hour_bucket', 'avg_bid', 'avg_ask']]
            .set_index('hour_bucket')
            .sort_index()
        )
        st.caption("میانگین قیمت خرید و فروش به تفکیک ساعت")
        st.line_chart(avg_chart)
    with g_col2:
        spread_range_chart = (
            gold_df[['hour_bucket', 'avg_spread', 'min_spread', 'max_spread']]
            .set_index('hour_bucket')
            .sort_index()
        )
        st.caption("دامنه اسپرد (کمینه، میانگین، بیشینه) به تفکیک ساعت")
        st.line_chart(spread_range_chart)

    with st.expander("جدول داده‌های Gold Layer"):
        st.dataframe(gold_df, use_container_width=True)

# ── Auto-refresh ──────────────────────────────────────────────────────────────
time.sleep(30)
st.rerun()
