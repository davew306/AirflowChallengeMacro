from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
import pandas as pd
import pyodbc 
import os
from datetime import datetime
import openpyxl

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Initialize the DAG
dag = DAG(
    'sqlserver_to_excel_with_macro',
    default_args=default_args,
    description='Pull data from SQL Server, save as Excel, run macro',
    schedule_interval='@daily',
)

def extract_data_from_sqlserver():
    # Retrieve SQL Server connection details from Airflow
    conn = BaseHook.get_connection('mssql_conn')
    
    # Establish connection to SQL Server using pyodbc
    connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={conn.host};DATABASE={conn.schema};UID={conn.login};PWD={conn.password}"
    connection = pyodbc.connect(connection_string)
    
    # SQL query to extract data
    query = "SELECT * FROM dbo.customers"
    
    # Load data into pandas DataFrame
    df = pd.read_sql(query, con=connection)
    connection.close()
    
    # Save DataFrame to Excel
    output_path = '/documents/airflow/input/first_output.xlsx'
    df.to_excel(output_path, index=False)
    
    # Return the path of the saved Excel file
    return output_path

def run_excel_macro(file_path):
    # Load the workbook with macros enabled
    xl = openpyxl.load_workbook(file_path, keep_vba=True)
    
    # Run the specified macro (replace 'YourMacroName' with the actual macro name)
    xl.run_macro('Format_Data')
    
    # Save the workbook after running the macro
    xl.save(file_path)

# Define the task to extract data from SQL Server
extract_task = PythonOperator(
    task_id='extract_data_from_sqlserver',
    python_callable=extract_data_from_sqlserver,
    dag=dag,
)

# Define the task to run the Excel macro
macro_task = PythonOperator(
    task_id='run_excel_macro',
    python_callable=run_excel_macro,
    op_args=['/documents/airflow/output/final_output.xlsx'],
    dag=dag,
)

# Set the task dependencies
extract_task >> macro_task