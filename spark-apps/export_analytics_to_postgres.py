"""
Export Daily Analytics to PostgreSQL

Reads analytics data from MinIO and exports to PostgreSQL for Grafana visualization
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime
import sys

print("=" * 70)
print("📊 DAILY ANALYTICS EXPORT TO POSTGRESQL")
print("=" * 70)
print(f"🕐 Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("")

# Create Spark Session
spark = (
    SparkSession.builder.appName("Daily Analytics Export")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

print("✅ Spark Session created")

# PostgreSQL connection
postgres_url = "jdbc:postgresql://postgres-analytics:5432/analytics_db"
postgres_properties = {
    "user": "analytics",
    "password": "analytics123",
    "driver": "org.postgresql.Driver",
}

print("✅ PostgreSQL connection configured")
print("")

# ==========================================
# 1. EXPORT TOP PRODUCTS
# ==========================================
print("📦 Reading top products from MinIO...")
try:
    top_products_path = "s3a://hudi-warehouse/top_products_daily"
    top_products = spark.read.format("parquet").load(top_products_path)

    print(f"✅ Read {top_products.count()} product records")
    print("Sample data:")
    top_products.show(5, truncate=False)

    print("💾 Exporting to PostgreSQL...")
    top_products.write.jdbc(
        url=postgres_url,
        table="daily_top_products",
        mode="overwrite",
        properties=postgres_properties,
    )
    print("✅ Top products exported to PostgreSQL")

except Exception as e:
    print(f"⚠️ Warning: Could not export top products - {e}")

print("")

# ==========================================
# 2. EXPORT TOP CUSTOMERS
# ==========================================
print("👥 Reading top customers from MinIO...")
try:
    top_customers_path = "s3a://hudi-warehouse/top_customers_daily"
    top_customers = spark.read.format("parquet").load(top_customers_path)

    print(f"✅ Read {top_customers.count()} customer records")
    print("Sample data:")
    top_customers.show(5, truncate=False)

    print("💾 Exporting to PostgreSQL...")
    top_customers.write.jdbc(
        url=postgres_url,
        table="daily_top_customers",
        mode="overwrite",
        properties=postgres_properties,
    )
    print("✅ Top customers exported to PostgreSQL")

except Exception as e:
    print(f"⚠️ Warning: Could not export top customers - {e}")

print("")

# ==========================================
# 3. EXPORT STATUS DISTRIBUTION
# ==========================================
print("📊 Reading status distribution from MinIO...")
try:
    status_path = "s3a://hudi-warehouse/status_distribution_daily"
    status_dist = spark.read.format("parquet").load(status_path)

    print(f"✅ Read {status_dist.count()} status records")
    print("Sample data:")
    status_dist.show(truncate=False)

    print("💾 Exporting to PostgreSQL...")
    status_dist.write.jdbc(
        url=postgres_url,
        table="daily_status_distribution",
        mode="overwrite",
        properties=postgres_properties,
    )
    print("✅ Status distribution exported to PostgreSQL")

except Exception as e:
    print(f"⚠️ Warning: Could not export status distribution - {e}")

print("")

# ==========================================
# 4. EXPORT HOURLY TRENDS
# ==========================================
print("⏰ Reading hourly trends from MinIO...")
try:
    hourly_path = "s3a://hudi-warehouse/hourly_trends_daily"
    hourly_trends = spark.read.format("parquet").load(hourly_path)

    print(f"✅ Read {hourly_trends.count()} hourly records")
    print("Sample data:")
    hourly_trends.show(10, truncate=False)

    print("💾 Exporting to PostgreSQL...")
    hourly_trends.write.jdbc(
        url=postgres_url,
        table="daily_hourly_trends",
        mode="overwrite",
        properties=postgres_properties,
    )
    print("✅ Hourly trends exported to PostgreSQL")

except Exception as e:
    print(f"⚠️ Warning: Could not export hourly trends - {e}")

print("")

# ==========================================
# 5. EXPORT OVERALL METRICS (from Hudi)
# ==========================================
print("📈 Reading overall metrics from MinIO...")
try:
    analytics_path = "s3a://hudi-warehouse/daily_analytics"
    overall_metrics = spark.read.format("hudi").load(analytics_path)

    print(f"✅ Read {overall_metrics.count()} metric records")
    print("Sample data:")
    overall_metrics.select(
        "processing_date",
        "total_orders",
        "total_revenue",
        "avg_order_value",
        "unique_customers",
        "unique_products",
    ).show(truncate=False)

    print("💾 Exporting to PostgreSQL...")
    overall_metrics.select(
        col("processing_date"),
        col("total_orders"),
        col("total_revenue"),
        col("avg_order_value"),
        col("min_order"),
        col("max_order"),
        col("unique_customers"),
        col("unique_products"),
    ).write.jdbc(
        url=postgres_url,
        table="daily_overall_metrics",
        mode="overwrite",
        properties=postgres_properties,
    )
    print("✅ Overall metrics exported to PostgreSQL")

except Exception as e:
    print(f"⚠️ Warning: Could not export overall metrics - {e}")

print("")

# ==========================================
# SUMMARY
# ==========================================
print("=" * 70)
print("🎉 DAILY ANALYTICS EXPORT COMPLETED!")
print("=" * 70)
print("")
print("📊 Exported Tables to PostgreSQL:")
print("   ✅ daily_top_products")
print("   ✅ daily_top_customers")
print("   ✅ daily_status_distribution")
print("   ✅ daily_hourly_trends")
print("   ✅ daily_overall_metrics")
print("")
print("🌐 Access Points:")
print("   • PostgreSQL: localhost:5433")
print("   • Grafana:    http://localhost:3000")
print("")
print("📝 Next Steps:")
print("   1. Verify tables in PostgreSQL")
print("   2. Create Grafana dashboard")
print("   3. Add to Airflow DAG for automation")
print("")
print("=" * 70)

spark.stop()
