import psycopg2
import time

try:
    print("Connecting to database to clear sessions...")
    conn = psycopg2.connect(host='postgres', database='airflow', user='airflow', password='airflow')
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE session CASCADE;")
    conn.commit()
    cursor.close()
    conn.close()
    print("Sessions cleared successfully.")
except Exception as e:
    print(f"Failed to clear sessions: {e}")
