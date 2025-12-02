"""
Test script to verify analytics pipeline can read from MinIO

This script tests:
1. Connection to MinIO
2. Reading Hudi tables
3. Basic analytics calculations
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

print("="*60)
print("🧪 Testing Daily Analytics Pipeline")
print("="*60)

# Create Spark Session
print("\n1️⃣ Creating Spark Session with MinIO configuration...")
spark = SparkSession.builder \
    .appName("Test Analytics Pipeline") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

print("✅ Spark Session created")

# Test reading from MinIO
print("\n2️⃣ Testing connection to MinIO...")
orders_path = "s3a://hudi-warehouse/orders_raw"

try:
    print(f"   Reading from: {orders_path}")
    orders_df = spark.read.format("hudi").load(orders_path)
    count = orders_df.count()
    print(f"✅ Successfully connected to MinIO!")
    print(f"   Found {count} orders in Hudi table")
except Exception as e:
    print(f"❌ Error reading from MinIO: {e}")
    print("\n💡 Troubleshooting:")
    print("   1. Make sure MinIO is running: docker ps | grep minio")
    print("   2. Verify data exists in MinIO UI: http://localhost:9001")
    print("   3. Run streaming job first to populate data")
    spark.stop()
    exit(1)

# Show sample data
print("\n3️⃣ Sample orders from MinIO:")
orders_df.show(5, truncate=False)

# Test basic analytics
print("\n4️⃣ Testing analytics calculations...")

metrics = orders_df.agg(
    count("order_id").alias("total_orders"),
    sum("total_amount").alias("total_revenue"),
    avg("total_amount").alias("avg_order_value")
)

print("✅ Metrics calculated:")
metrics.show(truncate=False)

# Test top products
print("\n5️⃣ Testing top products calculation...")
top_products = orders_df.groupBy("product_id").agg(
    count("order_id").alias("order_count"),
    sum("total_amount").alias("revenue")
).orderBy(col("revenue").desc()).limit(5)

print("✅ Top 5 products:")
top_products.show(truncate=False)

# Test write to MinIO
print("\n6️⃣ Testing write to MinIO...")
test_path = "s3a://hudi-warehouse/test_analytics"

try:
    metrics.withColumn("test_date", current_date()) \
        .write.format("parquet") \
        .mode("overwrite") \
        .save(test_path)
    print(f"✅ Successfully wrote to: {test_path}")
    
    # Verify read back
    test_read = spark.read.parquet(test_path)
    print(f"✅ Successfully read back {test_read.count()} records")
except Exception as e:
    print(f"❌ Error writing to MinIO: {e}")
    spark.stop()
    exit(1)

print("\n" + "="*60)
print("🎉 ALL TESTS PASSED!")
print("="*60)
print("\n✅ Your analytics pipeline is ready to run!")
print("\nNext steps:")
print("1. Copy files to airflow-dags/ and spark-apps/")
print("2. Access Airflow UI: http://localhost:8086")
print("3. Enable and trigger the 'daily_order_analytics' DAG")
print("4. Monitor execution and check results in MinIO")
print("\n" + "="*60)

spark.stop()
