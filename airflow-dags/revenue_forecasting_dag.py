"""
Revenue Forecasting DAG

Analyzes historical order data and generates 7-day revenue forecasts
using multiple forecasting methods with seasonality adjustments.

Schedule: Daily at 6 AM
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

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
    'revenue_forecasting',
    default_args=default_args,
    description='7-day revenue forecasting with seasonality analysis',
    schedule_interval='0 6 * * *',  # Daily at 6 AM
    catchup=False,
    tags=['forecasting', 'revenue', 'analytics', 'ml'],
)

# Task 1: Run revenue forecast
forecast_command = """
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/hudi-jars/hudi-spark3.5-bundle_2.12-0.15.0.jar,/opt/hudi-jars/hadoop-aws.jar,/opt/hudi-jars/aws-java-sdk-bundle.jar,/opt/hudi-jars/postgresql-42.7.1.jar \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=admin \
  --conf spark.hadoop.fs.s3a.secret.key=password123 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  /opt/spark-apps/revenue_forecast.py
"""

run_forecast = BashOperator(
    task_id='generate_revenue_forecast',
    bash_command=forecast_command,
    dag=dag,
)


def generate_forecast_report(**context):
    """Generate HTML forecast report"""
    from datetime import datetime
    
    report_date = context['ds']
    
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Revenue Forecast Report - {report_date}</title>
        <style>
            body {{
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                margin: 40px;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                min-height: 100vh;
            }}
            .container {{
                max-width: 1400px;
                margin: 0 auto;
                background-color: white;
                padding: 40px;
                border-radius: 15px;
                box-shadow: 0 10px 40px rgba(0,0,0,0.2);
            }}
            h1 {{
                color: #2c3e50;
                border-bottom: 4px solid #3498db;
                padding-bottom: 15px;
                font-size: 36px;
            }}
            h2 {{
                color: #34495e;
                margin-top: 30px;
                font-size: 24px;
            }}
            .header-info {{
                background: #f8f9fa;
                padding: 20px;
                border-radius: 8px;
                margin: 20px 0;
            }}
            .metric-row {{
                display: flex;
                gap: 20px;
                margin: 30px 0;
                flex-wrap: wrap;
            }}
            .metric-card {{
                flex: 1;
                min-width: 200px;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                padding: 25px;
                border-radius: 12px;
                box-shadow: 0 4px 15px rgba(0,0,0,0.1);
            }}
            .metric-card.secondary {{
                background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
            }}
            .metric-card.success {{
                background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
            }}
            .metric-label {{
                font-size: 14px;
                opacity: 0.9;
                text-transform: uppercase;
                letter-spacing: 1px;
            }}
            .metric-value {{
                font-size: 36px;
                font-weight: bold;
                margin-top: 10px;
            }}
            .metric-subtext {{
                font-size: 12px;
                opacity: 0.8;
                margin-top: 5px;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                margin: 25px 0;
                box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            }}
            th, td {{
                padding: 15px;
                text-align: left;
                border-bottom: 1px solid #ddd;
            }}
            th {{
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                font-weight: 600;
                text-transform: uppercase;
                font-size: 12px;
                letter-spacing: 1px;
            }}
            tr:hover {{
                background-color: #f5f5f5;
            }}
            tr:nth-child(even) {{
                background-color: #f8f9fa;
            }}
            .forecast-row {{
                font-weight: 500;
            }}
            .method-tag {{
                display: inline-block;
                padding: 5px 12px;
                background-color: #3498db;
                color: white;
                border-radius: 20px;
                font-size: 12px;
                font-weight: 600;
            }}
            .confidence-badge {{
                display: inline-block;
                padding: 5px 12px;
                background-color: #27ae60;
                color: white;
                border-radius: 20px;
                font-size: 12px;
                font-weight: 600;
            }}
            .info-box {{
                background: #e8f4f8;
                border-left: 4px solid #3498db;
                padding: 15px 20px;
                margin: 20px 0;
                border-radius: 4px;
            }}
            .warning-box {{
                background: #fff3cd;
                border-left: 4px solid #ffc107;
                padding: 15px 20px;
                margin: 20px 0;
                border-radius: 4px;
            }}
            .footer {{
                margin-top: 50px;
                text-align: center;
                color: #7f8c8d;
                font-size: 13px;
                padding-top: 20px;
                border-top: 2px solid #ecf0f1;
            }}
            .query-box {{
                background: #2c3e50;
                color: #ecf0f1;
                padding: 20px;
                border-radius: 8px;
                font-family: 'Courier New', monospace;
                font-size: 13px;
                overflow-x: auto;
                margin: 20px 0;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>📈 Revenue Forecast Report</h1>
            
            <div class="header-info">
                <p><strong>📅 Report Generated:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                <p><strong>🔮 Forecast Period:</strong> Next 7 Days</p>
                <p><strong>🎯 Forecast Method:</strong> <span class="method-tag">Ensemble Model</span></p>
                <p><strong>✅ Confidence Level:</strong> <span class="confidence-badge">85%</span></p>
            </div>

            <h2>📊 Forecast Summary</h2>
            <div class="metric-row">
                <div class="metric-card">
                    <div class="metric-label">7-Day Total Forecast</div>
                    <div class="metric-value">Loading...</div>
                    <div class="metric-subtext">Total predicted revenue</div>
                </div>
                <div class="metric-card secondary">
                    <div class="metric-label">Daily Average</div>
                    <div class="metric-value">Loading...</div>
                    <div class="metric-subtext">Average per day</div>
                </div>
                <div class="metric-card success">
                    <div class="metric-label">Growth Rate</div>
                    <div class="metric-value">Loading...</div>
                    <div class="metric-subtext">Week-over-week trend</div>
                </div>
            </div>

            <div class="info-box">
                <strong>ℹ️ About This Forecast:</strong> This forecast uses an ensemble of three methods:
                <ul style="margin: 10px 0 0 20px;">
                    <li><strong>Moving Average (MA):</strong> 7-day average with seasonality</li>
                    <li><strong>Weighted MA (WMA):</strong> Recent days weighted more heavily with growth adjustment</li>
                    <li><strong>Exponential Smoothing (ES):</strong> Balanced historical and recent trends</li>
                </ul>
                Final forecast is the average of all three methods with day-of-week seasonality applied.
            </div>

            <h2>📅 7-Day Revenue Forecast</h2>
            <table>
                <thead>
                    <tr>
                        <th>Date</th>
                        <th>Day</th>
                        <th>Forecasted Revenue</th>
                        <th>Lower Bound (85%)</th>
                        <th>Upper Bound (85%)</th>
                        <th>Seasonality Factor</th>
                    </tr>
                </thead>
                <tbody>
                    <tr><td colspan="6" style="text-align: center; padding: 20px;">Run query below to see actual forecast data</td></tr>
                </tbody>
            </table>

            <h2>📈 Day-of-Week Patterns</h2>
            <div class="info-box">
                The forecast accounts for different revenue patterns by day of week. 
                For example, weekends typically have different order volumes than weekdays.
            </div>

            <h2>🔍 How to View Forecast Data</h2>
            <p>Access detailed forecast results using PySpark:</p>
            
            <div class="query-box">
# Open PySpark shell
docker exec -it spark-master /opt/spark/bin/pyspark \\
  --jars /opt/hudi-jars/hudi-spark3.5-bundle_2.12-0.15.0.jar,... \\
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \\
  ...

# Read forecast
forecast = spark.read.parquet("s3a://hudi-warehouse/revenue_forecast")
forecast.show()

# Read summary
summary = spark.read.parquet("s3a://hudi-warehouse/revenue_forecast_summary")
summary.show()

# Read historical daily revenue
historical = spark.read.parquet("s3a://hudi-warehouse/historical_daily_revenue")
historical.orderBy(col("order_date").desc()).show(30)

# Read seasonality patterns
patterns = spark.read.parquet("s3a://hudi-warehouse/dow_seasonality_patterns")
patterns.show()
            </div>

            <h2>💾 Data Locations in MinIO</h2>
            <table>
                <thead>
                    <tr>
                        <th>Dataset</th>
                        <th>Location</th>
                        <th>Description</th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td>7-Day Forecast</td>
                        <td>s3a://hudi-warehouse/revenue_forecast</td>
                        <td>Daily predictions for next 7 days</td>
                    </tr>
                    <tr>
                        <td>Forecast Summary</td>
                        <td>s3a://hudi-warehouse/revenue_forecast_summary</td>
                        <td>Aggregated metrics and parameters</td>
                    </tr>
                    <tr>
                        <td>Historical Daily Revenue</td>
                        <td>s3a://hudi-warehouse/historical_daily_revenue</td>
                        <td>Past daily revenue for charting</td>
                    </tr>
                    <tr>
                        <td>Seasonality Patterns</td>
                        <td>s3a://hudi-warehouse/dow_seasonality_patterns</td>
                        <td>Day-of-week revenue patterns</td>
                    </tr>
                </tbody>
            </table>

            <div class="warning-box">
                <strong>⚠️ Important:</strong> Forecasts are predictions based on historical patterns. 
                Actual results may vary due to external factors, campaigns, holidays, or market conditions.
                Use as a guide for planning, not as guaranteed outcomes.
            </div>

            <h2>🎯 Use Cases</h2>
            <ul style="line-height: 2; font-size: 15px;">
                <li><strong>📦 Inventory Planning:</strong> Prepare stock based on predicted demand</li>
                <li><strong>👥 Staffing:</strong> Schedule employees for expected busy periods</li>
                <li><strong>💰 Cash Flow:</strong> Anticipate incoming revenue for financial planning</li>
                <li><strong>📊 KPI Tracking:</strong> Compare actual vs forecasted to identify anomalies</li>
                <li><strong>🎯 Goal Setting:</strong> Set realistic targets based on trends</li>
            </ul>

            <div class="footer">
                <p><strong>Generated by Airflow Revenue Forecasting Pipeline</strong></p>
                <p>Data Source: MinIO (s3a://hudi-warehouse/orders_raw)</p>
                <p>View data in MinIO UI: <a href="http://localhost:9001">http://localhost:9001</a></p>
                <p style="margin-top: 10px; font-size: 11px;">
                    Forecast updated daily at 6:00 AM | Next update: Tomorrow
                </p>
            </div>
        </div>
    </body>
    </html>
    """
    
    # Save report
    from pathlib import Path
    report_path = Path('/opt/airflow/dags')
    report_file = report_path / f'revenue_forecast_report_{report_date}.html'
    
    with open(report_file, 'w') as f:
        f.write(html_content)
    
    print(f"✅ Forecast report generated: {report_file}")
    return str(report_file)


generate_report = PythonOperator(
    task_id='generate_forecast_report',
    python_callable=generate_forecast_report,
    provide_context=True,
    dag=dag,
)


def send_forecast_summary(**context):
    """Send forecast summary notification"""
    report_date = context['ds']
    
    print("=" * 70)
    print(f"📈 REVENUE FORECAST SUMMARY - {report_date}")
    print("=" * 70)
    print("✅ 7-day revenue forecast completed successfully")
    print("")
    print("🔮 Forecast Generated:")
    print("   • Next 7 days revenue predictions")
    print("   • Day-by-day breakdown with confidence intervals")
    print("   • Seasonality adjustments applied")
    print("   • Multiple forecasting methods used (ensemble)")
    print("")
    print("📊 Forecast Methods:")
    print("   • Moving Average (7-day)")
    print("   • Weighted Moving Average (growth-adjusted)")
    print("   • Exponential Smoothing (trend-aware)")
    print("   • Ensemble average with 85% confidence level")
    print("")
    print("💾 Data saved to MinIO:")
    print("   • s3a://hudi-warehouse/revenue_forecast/")
    print("   • s3a://hudi-warehouse/revenue_forecast_summary/")
    print("   • s3a://hudi-warehouse/historical_daily_revenue/")
    print("   • s3a://hudi-warehouse/dow_seasonality_patterns/")
    print("")
    print("🌐 Access Points:")
    print("   • MinIO UI: http://localhost:9001")
    print("   • Airflow UI: http://localhost:8086")
    print("")
    print("📈 Use this forecast for:")
    print("   • Inventory planning")
    print("   • Staffing decisions")
    print("   • Cash flow projections")
    print("   • Performance tracking")
    print("=" * 70)


send_summary = PythonOperator(
    task_id='send_forecast_summary',
    python_callable=send_forecast_summary,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
run_forecast >> generate_report >> send_summary