"""
Daily Analytics Pipeline DAG

Automated pipeline that:
1. Runs daily analytics on orders from Hudi
2. Exports results to PostgreSQL for Grafana

Schedule: Daily at 7:00 AM
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Default arguments
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),  # Allow 30 minutes per task
}

# Create DAG
dag = DAG(
    "daily_analytics_pipeline",
    default_args=default_args,
    description="Daily order analytics with PostgreSQL export",
    schedule_interval="0 7 * * *",  # Daily at 7 AM
    catchup=False,
    tags=["analytics", "daily", "orders", "postgres"],
    dagrun_timeout=timedelta(hours=2),  # Max 2 hours for entire DAG
    max_active_runs=1,  # Only one run at a time
)

# ==========================================
# TASK 1: RUN DAILY ANALYTICS
# ==========================================
run_daily_analytics = BashOperator(
    task_id="run_daily_analytics",
    bash_command="""
    docker exec spark-master /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --jars /opt/hudi-jars/hudi-spark3.5-bundle_2.12-0.15.0.jar,/opt/hudi-jars/hadoop-aws.jar,/opt/hudi-jars/aws-java-sdk-bundle.jar \
      --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
      --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
      --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
      --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
      --conf spark.hadoop.fs.s3a.access.key=admin \
      --conf spark.hadoop.fs.s3a.secret.key=password123 \
      --conf spark.hadoop.fs.s3a.path.style.access=true \
      --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
      --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
      --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
      /opt/spark-apps/daily_analytics.py
    """,
    dag=dag,
)

# ==========================================
# TASK 2: EXPORT TO POSTGRESQL
# ==========================================
export_to_postgres = BashOperator(
    task_id="export_to_postgresql",
    bash_command="""
    docker exec spark-master /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --jars /opt/hudi-jars/hudi-spark3.5-bundle_2.12-0.15.0.jar,/opt/hudi-jars/hadoop-aws.jar,/opt/hudi-jars/aws-java-sdk-bundle.jar,/opt/hudi-jars/postgresql-42.7.1.jar \
      --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
      --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
      --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
      --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
      --conf spark.hadoop.fs.s3a.access.key=admin \
      --conf spark.hadoop.fs.s3a.secret.key=password123 \
      --conf spark.hadoop.fs.s3a.path.style.access=true \
      --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
      --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
      --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
      /opt/spark-apps/export_analytics_to_postgres.py
    """,
    dag=dag,
)

# ==========================================
# TASK 3: VERIFY POSTGRESQL EXPORT
# ==========================================
verify_postgres_export = BashOperator(
    task_id="verify_postgresql_export",
    bash_command="""
    docker exec postgres-analytics psql -U analytics -d analytics_db -c "
    SELECT 
        'daily_top_products' as table_name, 
        COUNT(*) as row_count 
    FROM daily_top_products
    UNION ALL
    SELECT 
        'daily_top_customers' as table_name, 
        COUNT(*) as row_count 
    FROM daily_top_customers
    UNION ALL
    SELECT 
        'daily_status_distribution' as table_name, 
        COUNT(*) as row_count 
    FROM daily_status_distribution
    UNION ALL
    SELECT 
        'daily_hourly_trends' as table_name, 
        COUNT(*) as row_count 
    FROM daily_hourly_trends
    UNION ALL
    SELECT 
        'daily_overall_metrics' as table_name, 
        COUNT(*) as row_count 
    FROM daily_overall_metrics;
    "
    """,
    dag=dag,
)


# ==========================================
# TASK 4: GENERATE SUMMARY REPORT
# ==========================================
def generate_summary(**context):
    """Generate analytics summary"""
    from datetime import datetime

    report_date = context["ds"]

    print("=" * 70)
    print(f"📊 DAILY ANALYTICS SUMMARY - {report_date}")
    print("=" * 70)
    print("")
    print("✅ Analytics Pipeline Completed Successfully!")
    print("")
    print("📦 Data Processing:")
    print("   • Orders analyzed from Hudi (MinIO)")
    print("   • Daily metrics calculated")
    print("   • Top products identified")
    print("   • Top customers ranked")
    print("   • Order status distribution computed")
    print("   • Hourly trends analyzed")
    print("")
    print("💾 Data Exported to PostgreSQL:")
    print("   ✅ daily_top_products")
    print("   ✅ daily_top_customers")
    print("   ✅ daily_status_distribution")
    print("   ✅ daily_hourly_trends")
    print("   ✅ daily_overall_metrics")
    print("")
    print("📊 Grafana Dashboards Updated:")
    print("   • Daily Analytics Dashboard")
    print("   • Access: http://localhost:3000")
    print("")
    print("🗄️ Data Sources:")
    print("   • MinIO (Data Lake): http://localhost:9001")
    print("   • PostgreSQL: localhost:5433")
    print("")
    print("📈 Use Cases:")
    print("   • Product performance tracking")
    print("   • Customer segmentation")
    print("   • Order flow monitoring")
    print("   • Hourly demand patterns")
    print("   • Business KPI tracking")
    print("")
    print("🔄 Next Run: Tomorrow at 7:00 AM")
    print("=" * 70)

    return "Summary generated successfully"


generate_summary_task = PythonOperator(
    task_id="generate_summary_report",
    python_callable=generate_summary,
    provide_context=True,
    dag=dag,
)


# ==========================================
# TASK 5: SEND SUCCESS NOTIFICATION
# ==========================================
def send_success_notification(**context):
    """Send success notification"""
    report_date = context["ds"]

    print("")
    print("🎉 " + "=" * 66)
    print("   DAILY ANALYTICS PIPELINE COMPLETED SUCCESSFULLY!")
    print("=" * 70)
    print("")
    print(f"📅 Processing Date: {report_date}")
    print(f"⏰ Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("")
    print("✅ All tasks executed successfully:")
    print("   1. ✅ Daily analytics computed")
    print("   2. ✅ Data exported to PostgreSQL")
    print("   3. ✅ Export verified")
    print("   4. ✅ Summary report generated")
    print("")
    print("📊 Dashboards are now updated with latest data!")
    print("🌐 View at: http://localhost:3000")
    print("")
    print("=" * 70)

    # In production, you would send email/Slack notification here
    # Example:
    # send_slack_message(f"✅ Daily analytics completed for {report_date}")
    # send_email(subject=f"Daily Analytics Report - {report_date}", body=summary)

    return "Notification sent"


send_notification = PythonOperator(
    task_id="send_success_notification",
    python_callable=send_success_notification,
    provide_context=True,
    dag=dag,
)

# ==========================================
# DEFINE TASK DEPENDENCIES
# ==========================================
(
    run_daily_analytics
    >> export_to_postgres
    >> verify_postgres_export
    >> generate_summary_task
    >> send_notification
)
