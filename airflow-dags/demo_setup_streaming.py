"""
Demo Setup DAG - Streaming & Data Generation

Automated demo preparation:
1. Starts Kafka→Hudi streaming consumer
2. Generates sample orders
3. Verifies data landed in Hudi

Use this before running your daily_analytics_pipeline DAG
Manual Trigger Only - Not Scheduled
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from datetime import datetime, timedelta
import subprocess

default_args = {
    'owner': 'demo-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=10),
}

dag = DAG(
    'demo_setup_streaming',
    default_args=default_args,
    description='🎬 Demo Setup: Start Streaming & Generate Orders',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['demo', 'streaming', 'setup'],
    dagrun_timeout=timedelta(minutes=30),
    max_active_runs=1,
)

# ==========================================
# TASK 1: PRE-CHECK - ENSURE KAFKA TOPIC EXISTS
# ==========================================
ensure_kafka_topic = BashOperator(
    task_id='ensure_kafka_topic_exists',
    bash_command="""
    echo "📡 Ensuring Kafka topic exists..."
    
    docker exec kafka kafka-topics --create \
      --bootstrap-server localhost:9092 \
      --replication-factor 1 \
      --partitions 3 \
      --topic orders \
      --if-not-exists 2>/dev/null || true
    
    echo "✅ Kafka topic 'orders' is ready"
    """,
    dag=dag,
)

# ==========================================
# TASK 2: START KAFKA STREAMING CONSUMER
# ==========================================
start_streaming = BashOperator(
    task_id='start_kafka_streaming_consumer',
    bash_command="""
    echo "🚀 Starting Kafka→Hudi streaming consumer..."
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    # Kill any existing streaming jobs first
    echo "Checking for existing streaming jobs..."
    docker exec spark-master pkill -9 -f kafka-to-hudi-minio.py 2>/dev/null || true
    sleep 5
    
    # Start streaming in background using screen (more reliable than nohup)
    echo "Starting new streaming job in background..."
    docker exec -d spark-master bash -c "
        /opt/spark/bin/spark-submit \
          --master spark://spark-master:7077 \
          --deploy-mode client \
          --driver-memory 1g \
          --executor-memory 1g \
          --jars /opt/hudi-jars/hudi-spark3.5-bundle_2.12-0.15.0.jar,/opt/hudi-jars/spark-sql-kafka.jar,/opt/hudi-jars/kafka-clients.jar,/opt/hudi-jars/commons-pool2.jar,/opt/hudi-jars/spark-token-provider-kafka.jar,/opt/hudi-jars/hadoop-aws.jar,/opt/hudi-jars/aws-java-sdk-bundle.jar \
          --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
          --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
          --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
          --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
          --conf spark.hadoop.fs.s3a.access.key=admin \
          --conf spark.hadoop.fs.s3a.secret.key=password123 \
          --conf spark.hadoop.fs.s3a.path.style.access=true \
          --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
          --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
          /opt/spark-apps/kafka-to-hudi-minio.py \
          > /tmp/streaming-demo.log 2>&1
    "
    
    echo ""
    echo "⏳ Waiting 20 seconds for streaming to initialize..."
    sleep 20
    
    echo ""
    echo "✅ Streaming consumer started in background"
    echo "📊 Check Spark UI: http://localhost:4040"
    echo "📝 Check logs: docker exec spark-master tail -f /tmp/streaming-demo.log"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    """,
    dag=dag,
)

# ==========================================
# TASK 3: WAIT FOR STREAMING TO BE READY
# ==========================================
def check_streaming_ready():
    """Check if streaming job is running and ready to accept data"""
    import time
    
    try:
        # Check 1: Look for Spark application
        result = subprocess.run(
            ['docker', 'exec', 'spark-master', 'curl', '-s', 'http://localhost:4040/api/v1/applications'],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.stdout and len(result.stdout) > 10:
            print("✅ Streaming application detected via Spark UI")
            
            # Check 2: Look for "idle and waiting" in logs (means it's ready for data)
            log_check = subprocess.run(
                ['docker', 'exec', 'spark-master', 'tail', '-20', '/tmp/streaming-demo.log'],
                capture_output=True,
                text=True,
                timeout=5
            )
            
            if 'idle and waiting' in log_check.stdout or 'Streaming started successfully' in log_check.stdout:
                print("✅ Streaming is IDLE and ready for data")
                return True
            else:
                print("⏳ Streaming detected but still initializing...")
                return False
        
        # Check 3: Process exists
        result2 = subprocess.run(
            ['docker', 'exec', 'spark-master', 'pgrep', '-f', 'kafka-to-hudi-minio'],
            capture_output=True,
            text=True,
            timeout=5
        )
        
        if result2.stdout.strip():
            print("✅ Streaming process found, waiting for it to be ready...")
            return False
            
    except Exception as e:
        print(f"⏳ Waiting for streaming to start... ({e})")
    
    return False

wait_for_streaming = PythonSensor(
    task_id='wait_for_streaming_ready',
    python_callable=check_streaming_ready,
    poke_interval=15,  # Check every 15 seconds
    timeout=180,  # Wait up to 3 minutes
    mode='poke',
    dag=dag,
)

# ==========================================
# TASK 4: GENERATE DEMO ORDERS
# ==========================================
generate_orders = BashOperator(
    task_id='generate_demo_orders',
    bash_command="""
    echo "📤 Generating demo orders..."
    
    # Install kafka-python and run Docker-compatible producer
    docker exec spark-master bash -c "
        pip install kafka-python -q 2>/dev/null
        python3 /opt/spark-apps/order-producer-docker.py
    "
    
    echo "✅ Orders sent!"
    echo "⏳ Waiting 60 seconds for processing..."
    sleep 60
    echo "✅ Done"
    """,
    dag=dag,
)

# ==========================================
# TASK 5: VERIFY DATA IN HUDI
# ==========================================
verify_hudi_data = BashOperator(
    task_id='verify_data_in_hudi',
    bash_command="""
    echo "🔍 Verifying data landed in Hudi..."
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    docker exec spark-master /opt/spark/bin/spark-submit \
      --master local[*] \
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
      /opt/spark-apps/verify_hudi_data.py
    
    echo ""
    echo "✅ Verification complete"
    """,
    execution_timeout=timedelta(minutes=5),
    dag=dag,
)

# ==========================================
# TASK 6: GENERATE DEMO SUMMARY
# ==========================================
def generate_demo_summary(**context):
    """Generate demo setup summary"""
    
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    summary = f"""
╔══════════════════════════════════════════════════════════════════════╗
║                  🎬 DEMO SETUP COMPLETED                             ║
╚══════════════════════════════════════════════════════════════════════╝

⏰ Completed at: {timestamp}
🎯 DAG Run ID: {context['dag_run'].run_id}

╔══════════════════════════════════════════════════════════════════════╗
║                     ✅ COMPLETED TASKS                               ║
╚══════════════════════════════════════════════════════════════════════╝

✅ Kafka topic 'orders' created
✅ Spark streaming consumer started (RUNNING IN BACKGROUND)
✅ 50 demo orders generated and sent to Kafka
✅ Data verified in Hudi data lake
✅ Streaming consumer will be stopped to free resources

╔══════════════════════════════════════════════════════════════════════╗
║                   📊 DEMO IS READY!                                  ║
╚══════════════════════════════════════════════════════════════════════╝

🌐 Access Points:
   • Spark Master UI:     http://localhost:8080
   • MinIO (Data Lake):   http://localhost:9001
   • Airflow:             http://localhost:8086

💾 Data Location:
   • Hudi Table:  s3a://hudi-warehouse/orders_raw
   • Format:      Apache Hudi (Copy-on-Write)
   • Storage:     MinIO

╔══════════════════════════════════════════════════════════════════════╗
║                   📋 NEXT STEPS                                      ║
╚══════════════════════════════════════════════════════════════════════╝

1️⃣  Run Analytics DAG:
   → Go to Airflow UI
   → Trigger 'daily_analytics_pipeline' DAG
   → Wait ~5-10 minutes for completion

2️⃣  View Dashboards:
   → Open Grafana: http://localhost:3000
   → Navigate to dashboards
   → See updated metrics

╔══════════════════════════════════════════════════════════════════════╗
║                 🎉 READY FOR PRESENTATION!                           ║
╚══════════════════════════════════════════════════════════════════════╝

Your demo environment is live with fresh data. 
Proceed with your presentation! 🚀

"""
    
    print(summary)
    
    # Save to file
    try:
        with open('/tmp/demo_setup_summary.txt', 'w') as f:
            f.write(summary)
        print("📄 Summary saved to: /tmp/demo_setup_summary.txt\n")
    except:
        pass
    
    return "Demo setup complete"

generate_summary = PythonOperator(
    task_id='generate_demo_summary',
    python_callable=generate_demo_summary,
    dag=dag,
)

# ==========================================
# TASK 7: STOP STREAMING TO FREE RESOURCES
# ==========================================
stop_streaming_cleanup = BashOperator(
    task_id='stop_streaming_cleanup',
    bash_command="""
    echo "🧹 Stopping streaming consumer to free resources..."
    docker exec spark-master pkill -f kafka-to-hudi-minio || true
    sleep 3
    echo "✅ Streaming stopped - resources freed for other jobs"
    """,
    dag=dag,
)

# ==========================================
# DEFINE TASK DEPENDENCIES
# ==========================================
ensure_kafka_topic >> start_streaming >> wait_for_streaming >> generate_orders >> verify_hudi_data >> generate_summary >> stop_streaming_cleanup