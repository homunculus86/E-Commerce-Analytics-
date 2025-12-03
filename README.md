
#  E-Commerce Real-Time Analytics & Revenue Forecasting Pipeline  
![Python](https://img.shields.io/badge/Python-FFD43B?logo=python&logoColor=blue)
![Grafana](https://img.shields.io/badge/Grafana-F47A20?logo=grafana&logoColor=white)
![Postgres](https://img.shields.io/badge/PostgreSQL-31648c?logo=postgresql&logoColor=white) 
![MinIO](https://img.shields.io/badge/MinIO-C72C48?logo=minio&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-017CEE?logo=apacheairflow&logoColor=white) 
![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white)
![Hudi](https://img.shields.io/badge/Apache%20Hudi-blue) 
![Spark](https://img.shields.io/badge/Apache%20Spark-orange?logo=apachespark)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-black?logo=apachekafka)

---

##  Project Overview

This project implements a **complete real-time data engineering pipeline** that ingests, processes, stores, analyzes, and visualizes e-commerce order data using a modern **Lakehouse architecture** with automated workflows and forecasting models.

The system demonstrates:
-  Real-time data ingestion (Kafka)
-  Stream processing & batch analytics (Spark)
-  ACID data lake with time travel (Apache Hudi on MinIO)
-  Automated DAG workflows (Airflow)
-  Business dashboards & revenue forecast (Grafana)
-  Data warehouse serving layer (PostgreSQL)
-  Fully containerized deployment (Docker Compose)

---


## System Architecture

![System Architecture](https://github.com/homunculus86/E-Commerce-Analytics-/blob/509c7f74c91d0383595059f2bac28c4066242d35/general%20architecture.png)

## Data workflow
```
              +----------------------+
              |   Data Generator     |
              | (synthetic orders)   |
              +----------+-----------+
                         |
                         ▼
                  +-------------+
                  |  KAFKA      |
                  +-------------+
                         |
            ┌────────────┴────────────┐
            |                          |
            ▼                          ▼
    Real-time Stream            Airflow Scheduled Batch
  +------------------+      +---------------------------+
  | Spark Structured |      | Spark Batch Jobs          |
  | Streaming        |      | Analytics + Forecasting   |
  +------------------+      +---------------------------+
            |                          |
            |                          |
            ▼                          ▼
      +-------------------+     +-------------------+
      | Apache Hudi       |     | PostgreSQL DB     |
      | (ACID Data Lake)  |     | Serving Layer     |
      +-------------------+     +-------------------+
                         |
                         └───────────► Grafana UI
```
---

##  Data Workflow

| Stage | Technology | Output |
|-------|-----------|--------|
| Real-time ingestion | Kafka | Event streaming |
| Transformation | Spark Streaming | Clean orders |
| ACID storage | Apache Hudi | Time travel + upsert |
| Batch analytics | Spark Batch | Metrics + aggregated stats |
| Forecasting | Python ML | Revenue predictions |
| Orchestration | Airflow | Daily automated workflows |
| Dashboards | Grafana | Metrics & Forecast |

---

##  Project Structure

```
 E-Commerce-Analytics
 ┣  spark-apps          --> batch + streaming spark jobs
 ┣  airflow-dags        --> DAG pipelines
 ┣  hudi-jars           --> hudi engine integration (you need to download the followings aws-java-sdk-bundle.ja rcommons-pool2.jar hadoop-aws.jar hudi-spark3.5-bundle_2.12-0.15.0.jar kafka-clients.jar postgresql-42.7.1.jar spark-sql-kafka.jar spark-token-provider-kafka.jar)
 ┣  airflow-logs
 ┣ .gitattributes
 ┣ docker-compose.yml     --> orchestrates the full system
```
---

##  Features

- Real-time processing under seconds ⏳  
- Time-travel on data lake ⏮  
- Update/delete row-level ACID transactions  
- Daily batch analytics & forecasts generated automatically  
- Grafana dashboards deployed out-of-the-box  
- End-to-end pipeline in **one command**  

---

##  How to Install & Run

###  Clone the Repository
```bash
git clone https://github.com/<YOUR_USERNAME>/E-Commerce-Analytics.git
cd E-Commerce-Analytics
```

###  Start the Entire Pipeline
>  Make sure **Docker Desktop** is installed and running

```bash
docker-compose up -d
```

###  Stop the Pipeline
```bash
docker-compose down
```

---

##  Access Web Interfaces

| Service | URL |
|--------|-----|
| Airflow UI | http://localhost:8080 |
| Kafka UI (if enabled) | http://localhost:8081 |
| MinIO | http://localhost:9000 |
| Grafana Dashboards | http://localhost:3000 |
| PostgreSQL | `localhost:5432` |

---

##  Dashboards Included

- Top Products & Customers
- Order Trend by Hour & Status
- Revenue Forecast + Confidence Bands
- Historical Metrics Comparison

---

##  Environment Variables  
Configure inside `docker-compose.yml` according to your credentials.

---
