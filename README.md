## Writing a DAG File for Apache Airflow in Python

Introduction

Apache Airflow is a powerful workflow orchestration platform that helps in managing complex data pipelines and tasks. DAGs, or Directed Acyclic Graphs, are the building blocks of Airflow, representing the workflow of tasks to be executed. DAG files are Python scripts that define the DAG structure, including tasks, dependencies, and scheduling information.



### Prerequisites
Ensure that you have Apache Airflow installed and running before creating a DAG file. You can install Airflow via pip:

```python
pip install apache-airflow
```

### Creating a Python File

1. Create a Python file within the Airflow DAGs directory. This directory is typically set in the Airflow configuration file (airflow.cfg).
Purpose: This step creates a Python file within the Airflow DAGs directory. This directory is typically configured in the Airflow configuration file ```(airflow.cfg)```. The DAG file will contain the DAG definition and task logic.

### Importing the Essential Modules

1. ```Importing datetime```: This module provides functions for working with dates and times, which is essential for scheduling DAG runs.

2. ```Importing airflow```: This module provides classes and functions for defining and managing DAGs.

3. ```Importing PythonOperator```: This class represents a task within a DAG and is used to define task logic and dependencies.

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


### Defining Default Arguments

1. Set default arguments for the DAG, specifying common settings like start date, schedule frequency, and owner:

default_args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1),
    'schedule_interval': '@daily',
}
```

### Instantiating the DAG

Instantiate the DAG object, providing the DAG ID, schedule information, and default arguments:

```Creating the DAG object:``` This step instantiates the DAG object using the ```DAG()``` class. The DAG object represents the overall workflow and manages the execution of tasks.

```DAG ID:``` This specifies a unique identifier for the DAG. It should be descriptive and consistent across DAGs.

```Default Arguments:``` This passes the default arguments defined previously to the DAG object.

```Schedule Interval:``` This specifies the schedule interval for the DAG, which is defined in the default arguments dictionary.

```python
dag = DAG('my_dag', default_args=default_args, schedule_interval='@daily')
```

Defining Default Arguments:

Setting default arguments: This section defines a dictionary of default arguments that can be applied to all tasks within the DAG. Common arguments include owner, start_date, and schedule_interval.

Owner: This specifies the owner of the DAG, typically the person or team responsible for maintaining it.

Start Date: This defines the starting date for the DAG, from which the schedule interval will be calculated.

Schedule Interval: This specifies the frequency at which the DAG should be executed. Common intervals include @daily, @hourly, and @once, indicating daily, hourly, and one-time execution, respectively.




### Creating Callable Functions for Tasks

Define callable functions for each task, encapsulating the task logic:

```python
def my_task_function():
    # Task logic implementation
    print("Executing my_task_function...")
    return 'Task completed successfully'
```

### Defining Tasks and Specifying Dependencies

Create tasks using PythonOperator, specifying the callable function and task ID:
```python
task1 = PythonOperator(task_id='my_task1', python_callable=my_task_function, dag=dag)
```

### Define dependencies between tasks using the >> operator:
```python
# Task dependency
task2 = PythonOperator(task_id='my_task2', python_callable=my_task_function, dag=dag)
task1 >> task2
```
### Running the DAG
Start the Airflow scheduler to execute the DAG according to the specified schedule.


## Here's a complete example of a DAG file in Python:
```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 19),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define a function for the first task
def task1_function():
    print("Executing task1_function...")
    return 'Task 1 completed successfully'

# Define a function for the second task
def task2_function():
    print("Executing task2_function...")
    return 'Task 2 completed successfully'

# Create a DAG object
dag = DAG('my_dag', default_args=default_args,catchup=False, schedule_interval='*/5 * * * *')

# Create tasks and specify their dependencies
task1 = PythonOperator(task_id='task1', python_callable=task1_function, dag=dag)
task2 = PythonOperator(task_id='task2', python_callable=task2_function, dag=dag)

# Set the dependency between tasks
task1 >> task2

```
Please keep in mind that the DAG object name should be the same as filename. ```'my_dag'```

The string ```*/5 * * * *``` represents a cron expression, which is a syntax used to schedule tasks at specific times or intervals. In this case, the expression ```*/5 * * * *``` indicates that the task should be executed every five minutes. The asterisk (*) wildcard in the minutes field means "every minute," and the slash (/) indicates that the task should be executed every five minutes.

### Another example of our server automation DAG code : 

```
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from clickhouse_driver import Client
import pymysql
from sqlalchemy import create_engine
import os
import psutil
import time
import shutil
from pathlib import Path
import pandas as pd
import numpy as np

default_args = {
    'owner': 'arash',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 15),
    'retries': 10,
    'retry_delay': timedelta(seconds=30),
}

dag = DAG('Server_automation',catchup=False, default_args=default_args, description='Your DAG description', schedule_interval='@daily')

def execute_all_clickhouse_queries():
    # Define your user, password, and host here
    user = 'root'
    password = 'Arash1648195'
    host = 'mysql_container2'
    database = 'pulse_check'
    engine = create_engine("mysql+pymysql://" + user + ":" + password + "@" + host + "/" + database)

    # Define your ClickHouse client here
    clickhouse_client = Client(host='172.21.16.1', port=9000, user='arash_khajooei', password='ABMu%Tfz3Cx#ob@369ES')

    execute_clickhouse_queries = [
        ("/opt/airflow/dags/pc_daily_query_v4.1.sql", "daily_view", 100000, 10),
        ("/opt/airflow/dags/pc_hourly_query_v4.1.sql", "hourly_view", 10000, 10),
        ("/opt/airflow/dags/pc_distance_query_v4.1.sql", "distance_view", 100000, 10),
        ("/opt/airflow/dags/pc_district_query_v3.sql", "district_view", 100000, 10)
    ]

    for sql_file, table_name, block_size, retries in execute_clickhouse_queries:
        for _ in range(retries):
            try:
                with open(sql_file, "r") as file:
                    query = file.read()

                result, columns = clickhouse_client.execute(query, with_column_types=True, settings={'max_block_size': block_size})
                data = pd.DataFrame(result, columns=[col[0] for col in columns])

                new_columns = [col.split('.')[1] if '.' in col else col for col in data.columns]
                data.columns = new_columns

                for col in data.columns:
                    if sum(data[col] == np.inf) > 0:
                        print(f"Column '{col}' has {sum(data[col] == np.inf)} inf values.")

                for col in data.columns:
                    if data[col].dtype != 'O':
                        data[col] = data[col].apply(lambda x: np.NaN if x == np.inf else x)

                data.to_sql(table_name, con=engine, if_exists='replace', index=False, chunksize=block_size)

                print(f"Query for {table_name} completed successfully.")
                break  # Exit the retry loop if the query was successful
            except Exception as e:
                print(f"Error executing query for {table_name}: {str(e)}")
                time.sleep(5)  # Wait for 5 seconds before retrying

execute_all_queries_task = PythonOperator(
    task_id='execute_all_queries',
    python_callable=execute_all_clickhouse_queries,
    dag=dag
)

execute_all_queries_task

```

# Explanation of Server Automation DAG

This DAG (`Server_automation`) in Apache Airflow is designed to execute multiple ClickHouse queries and push the results into a MySQL database (`pulse_check`). Let's break down the key components and steps within this DAG:

## Imports and Default Arguments

The DAG starts by importing necessary modules and defining default arguments like `owner`, `start_date`, `retries`, and `retry_delay`.

## DAG Configuration

The DAG itself (`Server_automation`) is instantiated with specific parameters:
- `catchup=False`: This ensures that Airflow doesn't attempt to run DAGs for time intervals in the past.
- `default_args=default_args`: This parameter includes the default arguments defined earlier.
- `description='Your DAG description'`: Description of the DAG (placeholder text, can be updated).
- `schedule_interval='@daily'`: The DAG is scheduled to run daily.

## Task: execute_all_clickhouse_queries()

This task is defined as a Python function (`execute_all_clickhouse_queries`) that handles executing ClickHouse queries and pushing results into MySQL.

### ClickHouse and MySQL Configuration

- `user`, `password`, `host`, and `database` variables: Define the credentials and database information for MySQL.
- `clickhouse_client`: Configures the ClickHouse client with the necessary connection details.
  
### ClickHouse Queries Execution

- `execute_clickhouse_queries`: A list of tuples containing SQL file paths, table names, block sizes, and retry counts.
  
### Looping Through Queries

The task iterates through each tuple in `execute_clickhouse_queries`:
- Opens the SQL file, reads the query, and executes it using the ClickHouse client.
- If successful, it converts the result into a Pandas DataFrame, handles infinite values, converts infinities to NaN, and pushes the data into the respective MySQL table.
- Retries the execution in case of failure, with a delay of 5 seconds between retries.

### PythonOperator Task

- `execute_all_queries_task`: Defines a `PythonOperator` task (`execute_all_queries`) using the defined Python function (`execute_all_clickhouse_queries`).

## Conclusion

This DAG automates the execution of multiple ClickHouse queries and stores the results in a MySQL database. Customization of credentials, query paths, or retry settings can be done based on specific project requirements.


