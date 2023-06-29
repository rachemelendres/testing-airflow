from datetime import datetime, timedelta
import os
import csv
from airflow import DAG
from airflow.operators.python import PythonOperator

# Function to extract data from the CSV file
def extract(directory, filename):
    # Read the CSV file and store its data
    with open(os.path.join(directory, filename), 'r') as file:
        data = list(csv.reader(file))
    print("Data in extract:", data)
    return data

# Function to transform the extracted data
def transform(data):
    print("Data in transform:", data)
    # Add a new column to the data
    transformed_data = [row + [int(row[1]) * 2] for row in data]
    return transformed_data

# Function to load the transformed data into a new CSV file
def load(transformed_data, output_directory, output_filename):
    with open(os.path.join(output_directory, output_filename), 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(transformed_data)


# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False
}

# Create the DAG
dag = DAG(
    'my_csv_pipeline',
    default_args=default_args,
    description='A CSV processing pipeline',
    schedule='0 * * * *',
    catchup=False
)

# Define the extract_task operator
extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract,
    op_args=[r'/path/to/airflow/dags/files', 'forex_currencies.csv'],
    dag=dag
)

# Define the transform_task operator
transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform,
    op_args=[extract_task.output],
    dag=dag
)

# Define the load_task operator
load_task = PythonOperator(
    task_id='load_task',
    python_callable=load,
    op_args=[transform_task.output, r'/path/to/airflow/dags/files', 'transformed_forex_currencies.csv'],
    dag=dag
)

# Set up task dependencies
extract_task >> transform_task >> load_task