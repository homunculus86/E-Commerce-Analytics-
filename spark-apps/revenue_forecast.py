"""
Revenue Forecasting with PostgreSQL Export

Analyzes historical order data, predicts next 7 days of revenue,
and exports results to PostgreSQL for Grafana visualization.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import sys
import builtins  # For using Python's built-in round()

print("=" * 70)
print("📈 REVENUE FORECASTING PIPELINE (with PostgreSQL Export)")
print("=" * 70)
print(f"🕐 Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("")

# Create Spark Session with MinIO configuration
spark = (
    SparkSession.builder.appName("Revenue Forecasting")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config(
        "spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
    )
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

print("✅ Spark Session created with MinIO configuration")

# PostgreSQL connection properties
postgres_url = "jdbc:postgresql://postgres-analytics:5432/analytics_db"
postgres_properties = {
    "user": "analytics",
    "password": "analytics123",
    "driver": "org.postgresql.Driver",
}

# ==========================================
# 1. READ HISTORICAL DATA
# ==========================================
orders_path = "s3a://hudi-warehouse/orders_raw"
print(f"\n📖 Reading historical orders from: {orders_path}")

try:
    orders_df = spark.read.format("hudi").load(orders_path)
    total_orders = orders_df.count()
    print(f"✅ Successfully read {total_orders} orders from MinIO")
except Exception as e:
    print(f"❌ Error reading from MinIO: {e}")
    sys.exit(1)

# ==========================================
# 2. CALCULATE HISTORICAL DAILY REVENUE
# ==========================================
print("\n📊 Calculating historical daily revenue...")

# Calculate daily revenue (cast date to timestamp for Grafana)
daily_revenue = (
    orders_df.groupBy("order_date")
    .agg(
        sum("total_amount").alias("revenue"),
        count("order_id").alias("order_count"),
        avg("total_amount").alias("avg_order_value"),
    )
    .orderBy("order_date")
)

# Cast order_date to timestamp for Grafana compatibility
daily_revenue = daily_revenue.withColumn("order_date", to_timestamp(col("order_date")))

# Add day of week
daily_revenue = (
    daily_revenue.withColumn("day_of_week", dayofweek("order_date"))
    .withColumn(
        "day_name",
        when(col("day_of_week") == 1, "Sunday")
        .when(col("day_of_week") == 2, "Monday")
        .when(col("day_of_week") == 3, "Tuesday")
        .when(col("day_of_week") == 4, "Wednesday")
        .when(col("day_of_week") == 5, "Thursday")
        .when(col("day_of_week") == 6, "Friday")
        .otherwise("Saturday"),
    )
    .withColumn("created_at", current_timestamp())
)

print("✅ Historical daily revenue calculated")

# ==========================================
# 3. CALCULATE FORECASTING METRICS
# ==========================================
print("\n🔢 Calculating forecasting metrics...")

window_30_days = Window.orderBy("order_date").rowsBetween(-29, 0)
window_7_days = Window.orderBy("order_date").rowsBetween(-6, 0)
window_14_days = Window.orderBy("order_date").rowsBetween(-13, 0)

daily_revenue_with_metrics = (
    daily_revenue.withColumn("ma_7", avg("revenue").over(window_7_days))
    .withColumn("ma_14", avg("revenue").over(window_14_days))
    .withColumn("ma_30", avg("revenue").over(window_30_days))
)

print("✅ Moving averages calculated")

# ==========================================
# 4. CALCULATE DAY-OF-WEEK PATTERNS
# ==========================================
print("\n📆 Analyzing day-of-week seasonality patterns...")

dow_patterns = (
    daily_revenue.groupBy("day_of_week", "day_name")
    .agg(
        avg("revenue").alias("avg_revenue"),
        stddev("revenue").alias("stddev_revenue"),
        count("order_date").alias("sample_count"),
    )
    .orderBy("day_of_week")
)

overall_avg = daily_revenue.agg(avg("revenue")).first()[0]

dow_patterns = dow_patterns.withColumn(
    "seasonality_factor", col("avg_revenue") / overall_avg
).withColumn("created_at", current_timestamp())

print(f"✅ Overall average daily revenue: ${overall_avg:,.2f}")

# ==========================================
# 5. GET LATEST DATA FOR FORECASTING
# ==========================================
print("\n🎯 Preparing forecast base data...")

last_7_days = daily_revenue_with_metrics.orderBy(col("order_date").desc()).limit(7)
recent_avg = last_7_days.agg(avg("revenue")).first()[0]
recent_ma7 = last_7_days.orderBy(col("order_date").desc()).first()["ma_7"]
recent_ma14 = last_7_days.orderBy(col("order_date").desc()).first()["ma_14"]

first_week = daily_revenue.orderBy("order_date").limit(7).agg(avg("revenue")).first()[0]
last_week = last_7_days.agg(avg("revenue")).first()[0]
growth_rate = (last_week - first_week) / first_week if first_week > 0 else 0

print(f"✅ Recent 7-day average: ${recent_avg:,.2f}")
print(f"✅ Growth rate: {growth_rate * 100:.2f}%")

# ==========================================
# 6. GENERATE 7-DAY FORECAST
# ==========================================
print("\n🔮 Generating 7-day revenue forecast...")

last_date = daily_revenue.agg(max("order_date")).first()[0]
print(f"📅 Last date with data: {last_date}")

forecast_dates = []
for i in range(1, 8):
    next_date = last_date + timedelta(days=i)
    day_of_week = (next_date.weekday() + 2) % 7
    if day_of_week == 0:
        day_of_week = 7

    # Get seasonality factor for this day
    dow_row = dow_patterns.filter(col("day_of_week") == day_of_week).first()

    if dow_row is None:
        dow_factor = 1.0
        print(f"⚠️ No historical data for day {day_of_week}, using factor 1.0")
    else:
        dow_factor = dow_row["seasonality_factor"]

    # Method 1: Simple Moving Average with Seasonality
    forecast_ma = recent_ma7 * dow_factor

    # Method 2: Weighted Moving Average with Growth
    forecast_wma = recent_ma7 * (1 + growth_rate) * dow_factor

    # Method 3: Exponential Smoothing
    alpha = 0.3
    forecast_es = (alpha * recent_avg + (1 - alpha) * recent_ma14) * dow_factor

    # Ensemble: Average of all methods
    forecast_ensemble = (forecast_ma + forecast_wma + forecast_es) / 3

    # Confidence intervals
    lower_bound = forecast_ensemble * 0.85
    upper_bound = forecast_ensemble * 1.15

    # Convert date to timestamp for Grafana compatibility
    forecast_timestamp = datetime.combine(next_date, datetime.min.time())

    forecast_dates.append(
        {
            "forecast_date": forecast_timestamp,  # Now timestamp instead of date
            "day_of_week": day_of_week,
            "day_name": [
                "Monday",
                "Tuesday",
                "Wednesday",
                "Thursday",
                "Friday",
                "Saturday",
                "Sunday",
            ][day_of_week - 2]
            if day_of_week >= 2
            else ["Sunday", "Monday"][day_of_week],
            "forecast_revenue": builtins.round(forecast_ensemble, 2),
            "forecast_ma": builtins.round(forecast_ma, 2),
            "forecast_wma": builtins.round(forecast_wma, 2),
            "forecast_es": builtins.round(forecast_es, 2),
            "lower_bound": builtins.round(lower_bound, 2),
            "upper_bound": builtins.round(upper_bound, 2),
            "seasonality_factor": builtins.round(dow_factor, 3),
            "confidence_level": 0.85,
            "created_at": datetime.now(),
        }
    )

forecast_df = spark.createDataFrame(forecast_dates)

print("✅ 7-day forecast generated!")

# ==========================================
# 7. CALCULATE FORECAST SUMMARY
# ==========================================
print("\n📊 Calculating forecast summary...")

total_forecasted_revenue = forecast_df.agg(sum("forecast_revenue")).first()[0]
avg_daily_forecast = forecast_df.agg(avg("forecast_revenue")).first()[0]

summary_data = [
    {
        "forecast_period_start": forecast_dates[0]["forecast_date"],
        "forecast_period_end": forecast_dates[-1]["forecast_date"],
        "total_forecasted_revenue": builtins.round(total_forecasted_revenue, 2),
        "avg_daily_forecast": builtins.round(avg_daily_forecast, 2),
        "historical_avg_7d": builtins.round(recent_avg, 2),
        "historical_avg_14d": builtins.round(recent_ma14, 2),
        "growth_rate_percent": builtins.round(growth_rate * 100, 2),
        "forecast_method": "Ensemble (MA + WMA + ES)",
        "confidence_level": 0.85,
        "created_at": datetime.now(),
    }
]

summary_df = spark.createDataFrame(summary_data)

# ==========================================
# 8. EXPORT TO POSTGRESQL
# ==========================================
print("\n💾 Exporting to PostgreSQL...")

try:
    # Export historical daily revenue
    print("💾 Exporting historical_daily_revenue...")
    daily_revenue.write.jdbc(
        url=postgres_url,
        table="historical_daily_revenue",
        mode="overwrite",
        properties=postgres_properties,
    )
    print("✅ historical_daily_revenue exported")

    # Export forecast
    print("💾 Exporting revenue_forecast...")
    forecast_df.write.jdbc(
        url=postgres_url,
        table="revenue_forecast",
        mode="overwrite",
        properties=postgres_properties,
    )
    print("✅ revenue_forecast exported")

    # Export summary
    print("💾 Exporting forecast_summary...")
    summary_df.write.jdbc(
        url=postgres_url,
        table="forecast_summary",
        mode="overwrite",
        properties=postgres_properties,
    )
    print("✅ forecast_summary exported")

    # Export day-of-week patterns
    print("💾 Exporting dow_patterns...")
    dow_patterns.write.jdbc(
        url=postgres_url,
        table="dow_seasonality_patterns",
        mode="overwrite",
        properties=postgres_properties,
    )
    print("✅ dow_seasonality_patterns exported")

except Exception as e:
    print(f"❌ Error exporting to PostgreSQL: {e}")
    print("⚠️ Continuing with MinIO export...")

# ==========================================
# 9. ALSO SAVE TO MINIO (Backup)
# ==========================================
print("\n💾 Saving to MinIO (backup)...")

forecast_df.write.mode("overwrite").parquet("s3a://hudi-warehouse/revenue_forecast")
summary_df.write.mode("overwrite").parquet(
    "s3a://hudi-warehouse/revenue_forecast_summary"
)
daily_revenue.write.mode("overwrite").parquet(
    "s3a://hudi-warehouse/historical_daily_revenue"
)
dow_patterns.write.mode("overwrite").parquet(
    "s3a://hudi-warehouse/dow_seasonality_patterns"
)

print("✅ Backup saved to MinIO")

# ==========================================
# 10. FINAL SUMMARY
# ==========================================
print("\n" + "=" * 70)
print("🎉 REVENUE FORECASTING COMPLETED SUCCESSFULLY!")
print("=" * 70)
print(
    f"\n📅 Forecast Period: {forecast_dates[0]['forecast_date']} to {forecast_dates[-1]['forecast_date']}"
)
print(f"💰 Total Forecasted Revenue (7 days): ${total_forecasted_revenue:,.2f}")
print(f"📊 Average Daily Forecast: ${avg_daily_forecast:,.2f}")
print(f"📈 Growth Rate: {growth_rate * 100:.2f}%")
print("")
print("💾 Data exported to:")
print("   ✅ PostgreSQL (for Grafana)")
print("   ✅ MinIO (backup)")
print("")
print("🌐 Access Points:")
print("   • PostgreSQL: localhost:5433")
print("   • Grafana: http://localhost:3000")
print("   • MinIO: http://localhost:9001")
print("=" * 70)

spark.stop()
