"""
Daily Order Analytics - Spark Job

Reads orders from Hudi tables in MinIO, calculates daily metrics,
and writes results back to MinIO.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import sys

print("🚀 Starting Daily Order Analytics Job...")
print(f"📅 Processing date: {datetime.now().strftime('%Y-%m-%d')}")

# Create Spark Session with MinIO configuration
spark = SparkSession.builder \
    .appName("Daily Order Analytics") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

print("✅ Spark Session created with MinIO configuration")

# Read orders from Hudi in MinIO
orders_path = "s3a://hudi-warehouse/orders_raw"
print(f"📖 Reading orders from MinIO: {orders_path}")

try:
    orders_df = spark.read.format("hudi").load(orders_path)
    print(f"✅ Successfully read {orders_df.count()} orders from MinIO")
except Exception as e:
    print(f"❌ Error reading from MinIO: {e}")
    print("💡 Make sure you have run the streaming job and have data in MinIO")
    sys.exit(1)

# Show sample data
print("\n📊 Sample orders:")
orders_df.show(5, truncate=False)

# Calculate processing date (yesterday for daily batch)
processing_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
print(f"\n🗓️  Processing orders for date: {processing_date}")

# Filter orders for the processing date
# Note: Adjust this based on your actual date field
daily_orders = orders_df.filter(
    col("order_date") == processing_date
)

print(f"📊 Found {daily_orders.count()} orders for {processing_date}")

# If no orders found, try getting all orders (for demo purposes)
if daily_orders.count() == 0:
    print("⚠️  No orders found for specific date, using all available orders for demo")
    daily_orders = orders_df

# ==========================================
# 1. OVERALL METRICS
# ==========================================
print("\n📈 Calculating overall metrics...")

overall_metrics = daily_orders.agg(
    count("order_id").alias("total_orders"),
    sum("total_amount").alias("total_revenue"),
    avg("total_amount").alias("avg_order_value"),
    min("total_amount").alias("min_order"),
    max("total_amount").alias("max_order"),
    countDistinct("customer_id").alias("unique_customers"),
    countDistinct("product_id").alias("unique_products")
).withColumn("processing_date", lit(processing_date))

print("✅ Overall metrics calculated:")
overall_metrics.show(truncate=False)

# ==========================================
# 2. TOP PRODUCTS
# ==========================================
print("\n🏆 Calculating top products...")

top_products = daily_orders.groupBy("product_id").agg(
    count("order_id").alias("order_count"),
    sum("total_amount").alias("total_revenue"),
    avg("total_amount").alias("avg_revenue"),
    sum("quantity").alias("total_quantity")
).orderBy(col("total_revenue").desc()).limit(10)

print("✅ Top 10 products:")
top_products.show(10, truncate=False)

# ==========================================
# 3. TOP CUSTOMERS
# ==========================================
print("\n👥 Calculating top customers...")

top_customers = daily_orders.groupBy("customer_id").agg(
    count("order_id").alias("order_count"),
    sum("total_amount").alias("total_spent"),
    avg("total_amount").alias("avg_order_value")
).orderBy(col("total_spent").desc()).limit(10)

print("✅ Top 10 customers:")
top_customers.show(10, truncate=False)

# ==========================================
# 4. ORDER STATUS DISTRIBUTION
# ==========================================
print("\n📊 Calculating order status distribution...")

status_distribution = daily_orders.groupBy("status").agg(
    count("order_id").alias("count"),
    sum("total_amount").alias("revenue")
).withColumn(
    "percentage",
    round(col("count") * 100.0 / sum("count").over(Window.partitionBy()), 2)
)

print("✅ Status distribution:")
status_distribution.show(truncate=False)

# ==========================================
# 5. HOURLY TRENDS (if timestamp available)
# ==========================================
print("\n⏰ Calculating hourly trends...")

hourly_trends = daily_orders.withColumn(
    "hour",
    hour(col("order_timestamp"))
).groupBy("hour").agg(
    count("order_id").alias("order_count"),
    sum("total_amount").alias("revenue")
).orderBy("hour")

print("✅ Hourly trends:")
hourly_trends.show(24, truncate=False)

# ==========================================
# SAVE RESULTS TO MINIO
# ==========================================
print("\n💾 Saving analytics results to MinIO...")

# Hudi options for analytics table
hudi_options = {
    'hoodie.table.name': 'daily_analytics',
    'hoodie.datasource.write.recordkey.field': 'processing_date',
    'hoodie.datasource.write.partitionpath.field': '',
    'hoodie.datasource.write.table.name': 'daily_analytics',
    'hoodie.datasource.write.operation': 'upsert',
    'hoodie.datasource.write.precombine.field': 'processing_date',
    'hoodie.metadata.enable': 'false',
    'hoodie.datasource.write.hive_style_partitioning': 'false',
}

# Save overall metrics
analytics_path = "s3a://hudi-warehouse/daily_analytics"
print(f"💾 Writing overall metrics to: {analytics_path}")

overall_metrics.write.format("hudi") \
    .options(**hudi_options) \
    .mode("append") \
    .save(analytics_path)

print("✅ Overall metrics saved")

# Save top products
products_path = "s3a://hudi-warehouse/top_products_daily"
print(f"💾 Writing top products to: {products_path}")

top_products.withColumn("processing_date", lit(processing_date)) \
    .write.format("parquet") \
    .mode("overwrite") \
    .save(products_path)

print("✅ Top products saved")

# Save top customers
customers_path = "s3a://hudi-warehouse/top_customers_daily"
print(f"💾 Writing top customers to: {customers_path}")

top_customers.withColumn("processing_date", lit(processing_date)) \
    .write.format("parquet") \
    .mode("overwrite") \
    .save(customers_path)

print("✅ Top customers saved")

# Save status distribution
status_path = "s3a://hudi-warehouse/status_distribution_daily"
print(f"💾 Writing status distribution to: {status_path}")

status_distribution.withColumn("processing_date", lit(processing_date)) \
    .write.format("parquet") \
    .mode("overwrite") \
    .save(status_path)

print("✅ Status distribution saved")

# Save hourly trends
hourly_path = "s3a://hudi-warehouse/hourly_trends_daily"
print(f"💾 Writing hourly trends to: {hourly_path}")

hourly_trends.withColumn("processing_date", lit(processing_date)) \
    .write.format("parquet") \
    .mode("overwrite") \
    .save(hourly_path)

print("✅ Hourly trends saved")

# ==========================================
# SUMMARY
# ==========================================
print("\n" + "="*60)
print("🎉 DAILY ANALYTICS COMPLETED SUCCESSFULLY!")
print("="*60)
print(f"📅 Processing Date: {processing_date}")
print(f"📊 Total Orders Processed: {overall_metrics.first()['total_orders']}")
print(f"💰 Total Revenue: ${overall_metrics.first()['total_revenue']:,.2f}")
print(f"📈 Average Order Value: ${overall_metrics.first()['avg_order_value']:,.2f}")
print(f"👥 Unique Customers: {overall_metrics.first()['unique_customers']}")
print(f"🛍️  Unique Products: {overall_metrics.first()['unique_products']}")
print("\n💾 Results saved to MinIO:")
print(f"   - Overall Metrics: {analytics_path}")
print(f"   - Top Products: {products_path}")
print(f"   - Top Customers: {customers_path}")
print(f"   - Status Distribution: {status_path}")
print(f"   - Hourly Trends: {hourly_path}")
print("\n✅ Check MinIO UI at http://localhost:9001")
print("="*60)

spark.stop()
