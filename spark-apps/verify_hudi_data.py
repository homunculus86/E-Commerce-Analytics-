"""
Verify Hudi Data - Standalone Script

Checks if data has landed in Hudi and displays statistics
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, avg

print("\n" + "="*70)
print("📊 HUDI DATA VERIFICATION")
print("="*70)

# Create Spark Session
spark = SparkSession.builder \
    .appName("Verify Hudi Data") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

try:
    # Read from Hudi
    print("\n📖 Reading from Hudi: s3a://hudi-warehouse/orders_raw")
    df = spark.read.format("hudi").load("s3a://hudi-warehouse/orders_raw")
    
    # Get count
    total_count = df.count()
    
    print(f"\n✅ Total Orders in Hudi: {total_count}")
    
    if total_count == 0:
        print("\n⚠️  WARNING: No orders found in Hudi!")
        print("   • Check if streaming consumer is running")
        print("   • Check Spark UI: http://localhost:4040")
        print("   • Check streaming logs: docker logs spark-master")
        raise Exception("No data in Hudi")
    
    if total_count < 50:
        print(f"\n⚠️  Warning: Only {total_count} orders found (expected 100)")
        print("   • Data may still be processing")
    else:
        print(f"\n✅ Sufficient data for demo ({total_count} orders)")
    
    # Show sample data
    print("\n" + "="*70)
    print("📦 SAMPLE ORDERS:")
    print("="*70)
    df.select("order_id", "customer_id", "product_id", "total_amount", "status", "order_date") \
      .show(10, truncate=False)
    
    # Show statistics
    print("\n" + "="*70)
    print("📈 ORDER STATISTICS:")
    print("="*70)
    
    stats = df.agg(
        count("*").alias("total_orders"),
        sum("total_amount").alias("total_revenue"),
        avg("total_amount").alias("avg_order_value")
    ).first()
    
    print(f"  • Total Orders:      {stats['total_orders']}")
    print(f"  • Total Revenue:     ${stats['total_revenue']:,.2f}")
    print(f"  • Avg Order Value:   ${stats['avg_order_value']:,.2f}")
    
    # Status breakdown
    print("\n📊 ORDER STATUS BREAKDOWN:")
    df.groupBy("status").count().orderBy("count", ascending=False).show()
    
    print("\n" + "="*70)
    print("✅ VERIFICATION COMPLETE - DATA IS READY!")
    print("="*70)
    print("\n💡 Next Steps:")
    print("   1. Run 'daily_analytics_pipeline' DAG")
    print("   2. Check Grafana dashboards")
    print("   3. Query data in PySpark")
    print("")
    
except Exception as e:
    print(f"\n❌ ERROR: {e}")
    print("\n🔍 Troubleshooting:")
    print("   • Check if Spark streaming is running")
    print("   • Verify Kafka has messages")
    print("   • Check MinIO is accessible")
    raise

spark.stop()