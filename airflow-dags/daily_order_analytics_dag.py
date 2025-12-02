"""
Daily Order Analytics Pipeline - Airflow DAG (Fixed Version)

Uses BashOperator with docker exec to run spark-submit on the spark-master container.
This avoids needing Java/Spark installed in the Airflow container.

Reads orders from Hudi tables in MinIO, calculates daily metrics,
and generates analytics reports.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pathlib import Path

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'daily_order_analytics',
    default_args=default_args,
    description='Daily order analytics pipeline reading from MinIO',
    schedule_interval='0 0 * * *',  # Run daily at midnight
    catchup=False,
    tags=['analytics', 'orders', 'minio', 'hudi'],
)

# Task 1: Calculate Daily Metrics using spark-submit via docker exec
calculate_metrics_command = """
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/hudi-jars/hudi-spark3.5-bundle_2.12-0.15.0.jar,/opt/hudi-jars/hadoop-aws.jar,/opt/hudi-jars/aws-java-sdk-bundle.jar \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=admin \
  --conf spark.hadoop.fs.s3a.secret.key=password123 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  /opt/spark-apps/daily_analytics.py
"""

calculate_metrics = BashOperator(
    task_id='calculate_daily_metrics',
    bash_command=calculate_metrics_command,
    dag=dag,
)


def generate_report(**context):
    """Generate HTML report from analytics data"""
    from datetime import datetime
    
    report_date = context['ds']
    
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Daily Order Analytics - {report_date}</title>
        <style>
            body {{
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                margin: 40px;
                background-color: #f5f5f5;
            }}
            .container {{
                max-width: 1200px;
                margin: 0 auto;
                background-color: white;
                padding: 30px;
                border-radius: 10px;
                box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            }}
            h1 {{
                color: #2c3e50;
                border-bottom: 3px solid #3498db;
                padding-bottom: 10px;
            }}
            .metric-card {{
                display: inline-block;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                padding: 20px;
                margin: 10px;
                border-radius: 8px;
                min-width: 200px;
                box-shadow: 0 4px 6px rgba(0,0,0,0.1);
            }}
            .metric-label {{
                font-size: 14px;
                opacity: 0.9;
            }}
            .metric-value {{
                font-size: 32px;
                font-weight: bold;
                margin-top: 10px;
            }}
            .section {{
                margin-top: 30px;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                margin-top: 20px;
            }}
            th, td {{
                padding: 12px;
                text-align: left;
                border-bottom: 1px solid #ddd;
            }}
            th {{
                background-color: #3498db;
                color: white;
            }}
            tr:hover {{
                background-color: #f5f5f5;
            }}
            .footer {{
                margin-top: 40px;
                text-align: center;
                color: #7f8c8d;
                font-size: 12px;
            }}
            .success {{
                color: #27ae60;
                font-weight: bold;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>📊 Daily Order Analytics Report</h1>
            <p><strong>Report Date:</strong> {report_date}</p>
            <p><strong>Generated:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            <p class="success">✅ Analytics pipeline completed successfully!</p>
            
            <div class="section">
                <h2>📈 Key Metrics Generated</h2>
                <div class="metric-card">
                    <div class="metric-label">Overall Metrics</div>
                    <div class="metric-value">✓</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">Top Products</div>
                    <div class="metric-value">✓</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">Top Customers</div>
                    <div class="metric-value">✓</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">Status Distribution</div>
                    <div class="metric-value">✓</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">Hourly Trends</div>
                    <div class="metric-value">✓</div>
                </div>
            </div>
            
            <div class="section">
                <h2>💾 Data Locations in MinIO</h2>
                <table>
                    <thead>
                        <tr>
                            <th>Analytics Table</th>
                            <th>Location</th>
                            <th>Description</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td>Overall Metrics</td>
                            <td>s3a://hudi-warehouse/daily_analytics/</td>
                            <td>Total orders, revenue, AOV, unique customers/products</td>
                        </tr>
                        <tr>
                            <td>Top Products</td>
                            <td>s3a://hudi-warehouse/top_products_daily/</td>
                            <td>Best-selling products by revenue</td>
                        </tr>
                        <tr>
                            <td>Top Customers</td>
                            <td>s3a://hudi-warehouse/top_customers_daily/</td>
                            <td>Highest-spending customers</td>
                        </tr>
                        <tr>
                            <td>Status Distribution</td>
                            <td>s3a://hudi-warehouse/status_distribution_daily/</td>
                            <td>Order status breakdown (confirmed/pending/cancelled)</td>
                        </tr>
                        <tr>
                            <td>Hourly Trends</td>
                            <td>s3a://hudi-warehouse/hourly_trends_daily/</td>
                            <td>Orders and revenue by hour of day</td>
                        </tr>
                    </tbody>
                </table>
            </div>
            
            <div class="section">
                <h2>🔍 How to Query Results</h2>
                <p><strong>Using PySpark:</strong></p>
                <pre style="background-color: #f4f4f4; padding: 15px; border-radius: 5px; overflow-x: auto;">
# Read overall metrics
metrics = spark.read.format("hudi").load("s3a://hudi-warehouse/daily_analytics")
metrics.show()

# Read top products
products = spark.read.parquet("s3a://hudi-warehouse/top_products_daily")
products.show()

# Read top customers
customers = spark.read.parquet("s3a://hudi-warehouse/top_customers_daily")
customers.show()
                </pre>
            </div>
            
            <div class="footer">
                <p>Generated by Airflow Daily Order Analytics Pipeline</p>
                <p>Data Source: MinIO (s3a://hudi-warehouse/orders_raw)</p>
                <p>View data in MinIO UI: <a href="http://localhost:9001">http://localhost:9001</a></p>
            </div>
        </div>
    </body>
    </html>
    """
    
    # Save report to airflow dags directory (accessible from host)
    report_path = Path('/opt/airflow/dags')
    report_file = report_path / f'order_analytics_report_{report_date}.html'
    
    with open(report_file, 'w') as f:
        f.write(html_content)
    
    print(f"✅ Report generated: {report_file}")
    print(f"📊 View report at: http://localhost:8086/static/order_analytics_report_{report_date}.html")
    return str(report_file)


generate_report_task = PythonOperator(
    task_id='generate_html_report',
    python_callable=generate_report,
    provide_context=True,
    dag=dag,
)


def send_summary(**context):
    """Send summary notification"""
    report_date = context['ds']
    print("=" * 60)
    print(f"📧 Daily Analytics Summary for {report_date}")
    print("=" * 60)
    print("✅ Daily analytics completed successfully")
    print("")
    print("📊 Analytics Generated:")
    print("   • Overall business metrics")
    print("   • Top 10 products by revenue")
    print("   • Top 10 customers by spending")
    print("   • Order status distribution")
    print("   • Hourly order trends")
    print("")
    print("💾 Data saved to MinIO:")
    print("   • s3a://hudi-warehouse/daily_analytics/")
    print("   • s3a://hudi-warehouse/top_products_daily/")
    print("   • s3a://hudi-warehouse/top_customers_daily/")
    print("   • s3a://hudi-warehouse/status_distribution_daily/")
    print("   • s3a://hudi-warehouse/hourly_trends_daily/")
    print("")
    print("🌐 Access Points:")
    print("   • MinIO UI: http://localhost:9001")
    print("   • Airflow UI: http://localhost:8086")
    print("=" * 60)


send_summary_task = PythonOperator(
    task_id='send_summary',
    python_callable=send_summary,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
calculate_metrics >> generate_report_task >> send_summary_task
