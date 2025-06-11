import airflow
import json
import os
import csv
import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
from pathlib import Path
import psycopg2
import subprocess
import json
import psycopg2
from airflow.hooks.base import BaseHook
import os
import csv
import boto3
from datetime import timedelta, datetime
from pathlib import Path
import psycopg2
from psycopg2 import OperationalError
import pendulum




HOME_DIR = "/opt/airflow/"

#insert your mount folder
MOUNT_FOLDER = os.path.join(HOME_DIR, "s3-bucket")
# ==============================================================

# The default arguments for your Airflow, these have no reason to change for the purposes of this predict.
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': pendulum.today('UTC').add(days=-1)
}


# The function that uploads data to the RDS database, it is called upon later.

def upload_to_postgres(**kwargs):
	load_dotenv()  # Load environment variables from a .env file

# Database connection parameters
RDS_HOST = os.getenv("RDS_HOST", "ptde2403-mbd-jackson-mothapo-db-instance.cyg5kxo7cs9q.eu-west-1.rds.amazonaws.com")
RDS_PORT = os.getenv("RDS_PORT", "5432")
RDS_DB_NAME = os.getenv("RDS_DB_NAME", "ptde2403_mbd_jackson_mothapo_database")
RDS_USER = os.getenv("RDS_USER", "postgresAdmin")
RDS_PASSWORD = os.getenv("RDS_PASSWORD", "postgresAdmin")


	

connection = psycopg2.connect(
            host=RDS_HOST,
            port=RDS_PORT,
            database=RDS_DB_NAME,
            user=RDS_USER,
            password=RDS_PASSWORD,
            connect_timeout=30
    )
	
cursor = connection.cursor()
csv_file_path ="/home/ubuntu/s3-bucket/Output/historical_stock_data.csv"

with open(csv_file_path, 'r') as file:
            next(file)  # Skip the header row
            for line in file:
                columns = line.strip().split(',')
                if len(columns) == 10:
                # Use the copy_from method to load the data from the file into the database table
                    cursor.copy_from(file, 'historical_stocks_data', sep=',', columns=(
                        'stock_date', 'open_value', 'high_value', 'low_value', 'close_value', 
                        'volume_traded','openint','daily_percent_change', 'value_change','company_name'))
                else:
                    print(f"Skipping malformed line: {line}")


connection.commit()
print(f"Data from {csv_file_path} has been successfully loaded into the database.")

def failure_sns(context):

	# Write a function that will send a failure SNS notificaiton
	sns_client = boto3.client('sns', region_name='eu-west-1')
	topic_arn = "arn:aws:sns:eu-west-1:445492270995:PTDE2403-mbd-predict-jackson-mothapo-SNS"

	subject = f"{context['dag'].dag_id}_Pipeline_Failure"
	message = f"Task {context['task_instance'].task_id} failed in DAG {context['dag'].dag_id}. Please check the logs for details."

	sns_client.publish(
		TopicArn=topic_arn,
		Message=message,
		Subject=subject
	)

	return "Failure SNS Sent"

def success_sns(context):

	# Write a function that will send a success SNS Notification
	sns_client = boto3.client('sns', region_name='eu-west-1')
	topic_arn = "arn:aws:sns:eu-west-1:445492270995:PTDE2403-mbd-predict-jackson-mothapo-SNS"

	subject = f"{context['dag'].dag_id}_Pipeline_Success"
	message = f"Task {context['task_instance'].task_id} completed successfully in DAG {context['dag'].dag_id}."

	sns_client.publish(
		TopicArn=topic_arn,
		Message=message,
		Subject=subject
	)

	return "Success SNS sent"
# The dag configuration ===========================================================================
# Ensure your DAG calls on the success and failure functions above as it succeeds or fails.

with DAG('data_pipeline', 
         default_args=default_args, 
         description='A simple data pipeline with SNS notifications', 
         schedule='@daily',
         catchup=False,
        ) as dag:
	

# Write your DAG tasks below ============================================================
	
	# Task 1: Upload CSV to Postgres
    upload_task = PythonOperator(
        task_id='upload_to_postgres',
        python_callable=upload_to_postgres,
        op_args=['/home/ubuntu/s3-bucket/Output/historical_stock_data.csv'],   # Provide the path to your CSV file
        on_failure_callback=failure_sns,
        on_success_callback=success_sns,  
        
    )

	# Task 2: Send Success SNS Notification
    success_task = PythonOperator(
        task_id='send_success_sns',
        python_callable=success_sns,
        on_failure_callback=failure_sns,
        on_success_callback=success_sns,
        
    )

	# Task 3: Send Failure SNS Notification (This will trigger if any task fails)
    failure_task = PythonOperator(
        task_id='send_failure_sns',
        python_callable=failure_sns,
        on_failure_callback=failure_sns,
        on_success_callback=success_sns,
        
    )

	# Define your Task flow below ===========================================================
    upload_task >> success_task
    upload_task >> failure_task







