# Crypto Data Engineering Pipeline 🚀

Welcome to the Crypto Data Engineering Pipeline! This project is designed as an educational, end-to-end data pipeline to demonstrate modern Data Engineering concepts.

## 🏗️ Architecture (Medallion Architecture)

This project follows the Medallion Architecture pattern:

1. **Nobitex API (Source):** Live cryptocurrency orderbook data (BTC/IRT).
2. **MinIO / S3 (Bronze Layer 🥉):** Raw, unprocessed JSON data is extracted from the API and stored directly in a MinIO bucket (`crypto-raw-data`). This ensures we always have a historical record of the raw data.
3. **Apache Airflow (Orchestrator):** Schedules and runs the pipeline tasks. We use **XComs** (Cross-Task Communication) to pass the generated filename from the extraction task to the processing task.
4. **Pandas (Transformation):** Airflow reads the raw JSON from MinIO and uses the Pandas library to rapidly process and calculate the best bid, best ask, and spread.
5. **PostgreSQL (Silver Layer 🥈):** The cleaned, structured data is loaded into a PostgreSQL table (`silver_orderbook`) for downstream analytics and dashboards.
6. **Dashboards:** Data is served to Streamlit and Metabase for visualization.

## 🚀 Getting Started

The entire stack is containerized using Docker Compose for a seamless setup.

### Prerequisites
- Docker and Docker Compose installed on your machine.

### Running the Project

1. Navigate to the project directory:
   ```bash
   cd airflow_project
   ```

2. Start the unified stack (Airflow, Postgres, MinIO, Metabase, Streamlit):
   ```bash
   docker compose up -d
   ```

3. The services will be available at:
   - **Airflow UI:** `http://localhost:8080` (Username/Password: `admin`/`admin`)
   - **MinIO UI:** `http://localhost:9001` (Username/Password: `admin`/`password123`)
   - **Metabase:** `http://localhost:3000`
   - **Streamlit:** `http://localhost:8501`

## 🛠️ Lessons Learned & Common Gotchas

### 1. Airflow Python Dependencies (`pip install`)
**Issue:** If you try to manually run `docker exec -u root airflow_scheduler pip install pandas`, it will fail due to permission issues and is generally an anti-pattern in Docker.
**Solution:** The correct way to install packages in the official Apache Airflow Docker image is by using the `_PIP_ADDITIONAL_REQUIREMENTS` environment variable in `docker-compose.yml`. We added `pandas psycopg2-binary boto3` there, and Airflow auto-installs them gracefully upon startup.

### 2. Docker Networking vs. Localhost
**Issue:** In the Python DAG, the S3 endpoint was initially set to `http://host.docker.internal:9000`. This works inconsistently (often failing on Linux) because it tries to break out of the Docker network to hit the host machine.
**Solution:** By merging the MinIO service into the same `docker-compose.yml` file, all containers join the same internal Docker network. We then updated the DAG to simply call the service name: `http://minio:9000`.

### 3. MinIO Bucket Initialization
**Issue:** Attempting to put an object into an S3 bucket that doesn't exist throws a `NoSuchBucket` error.
**Solution:** We added defensive programming in the DAG (`try/except s3_client.head_bucket`) to automatically create the `crypto-raw-data` bucket on the first run if it's missing.

### 4. Streamlit PyPI Mirror Errors
**Issue:** Streamlit failed to build due to a DNS/name resolution error when attempting to reach `mirror.arvancloud.ir`.
**Solution:** We removed the custom pip mirror flag (`-i ...`) from the `streamlit` service command in `docker-compose.yml`, allowing it to fall back to the default, stable PyPI servers.

---
*Happy Data Engineering!* 🎓
