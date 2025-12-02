from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

print("🚀 Starting Kafka to Hudi (MinIO) Streaming with PySpark...")

# Create Spark Session with MinIO configuration
spark = SparkSession.builder \
    .appName("Kafka to Hudi MinIO Streaming") \
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

# Define order schema
order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("order_timestamp", StringType(), True),
    StructField("status", StringType(), True)
])

print("📖 Reading from Kafka topic: orders")

# Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON and transform
orders_df = kafka_df \
    .select(from_json(col("value").cast("string"), order_schema).alias("order")) \
    .select("order.*") \
    .withColumn("total_amount", col("quantity") * col("price")) \
    .withColumn("processing_time", current_timestamp()) \
    .withColumn("order_date", to_date(col("order_timestamp")))

print("🔄 Transformations applied")

# Hudi options
hudi_options = {
    'hoodie.table.name': 'orders_raw_minio',
    'hoodie.datasource.write.recordkey.field': 'order_id',
    'hoodie.datasource.write.partitionpath.field': '',
    'hoodie.datasource.write.table.name': 'orders_raw_minio',
    'hoodie.datasource.write.operation': 'upsert',
    'hoodie.datasource.write.precombine.field': 'order_timestamp',
    'hoodie.metadata.enable': 'false',
    'hoodie.datasource.write.hive_style_partitioning': 'false',
    'path': 's3a://hudi-warehouse/orders_raw'
}

print("💾 Writing to MinIO (s3a://hudi-warehouse/orders_raw)")

# Write to Hudi in MinIO
query = orders_df.writeStream \
    .format("hudi") \
    .options(**hudi_options) \
    .option("checkpointLocation", "/tmp/checkpoint/orders") \
    .trigger(processingTime='10 seconds') \
    .outputMode("append") \
    .start()

print("✅ Streaming started successfully!")
print("📊 Spark UI: http://localhost:4040")
print("💾 MinIO UI: http://localhost:9001")
print("⏹️  Press Ctrl+C to stop")
print("")
print("Waiting for data from Kafka...")

query.awaitTermination()