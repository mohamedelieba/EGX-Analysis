from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
from bs4 import BeautifulSoup
import requests
import pandas as pd
import re

URL = "https://www.tradingview.com/markets/stocks-egypt/market-movers-large-cap/"

def extract_data():
    resp = requests.get(URL)
    soup = BeautifulSoup(resp.text)
    symbols = []
    for i, tag in enumerate(soup.findAll('a', attrs={'class': re.compile('^apply-common-tooltip')})): 
        if(i%2==0 and tag.string != 'EGS923M1C017'):
            print(tag.string)
            symbols.append(tag.string)
    soup = BeautifulSoup(resp.text, 'html.parser')

# Find the table
    table = soup.find('table', class_='table-Ngq2xrcG')

    # Extract headers

    headers = [header.text.strip() for header in table.find_all('th')]

    # Extract rows
    rows = []
    print (headers)
    with open('/home/elieba/data/EGX-30_Data.csv', 'w') as file:
        for row in table.find_all('tr', class_='row-RdUXZpkv listRow'):
            cells = [cell.text.strip() for cell in row.find_all('td')]
            cleaned_cells = [value.replace(',', '_') for value in cells]
            data_record = ",".join(cleaned_cells)
            print(data_record)
            file.write(data_record)
            file.write("\n")
    df = pd.read_csv('~/data/EGX-30_Data.csv')
    
with DAG("EGX-Pipeline",
          start_date=datetime(2025, 3, 7),
          schedule="@daily",
          catchup = False) as dag:
    
    extract_task = PythonOperator(
        task_id="Extract_Data",
        python_callable=extract_data,
    )

    process_task = BashOperator(
        task_id = 'Process_Data',
        bash_command="""
       spark-submit \
       --master spark://de-bootcamp.us-central1-b.c.alexy-de-bootcamp.internal:7077 \
       --executor-cores 2 \
       --executor-memory 4G \
       --driver-memory 2G \
       /home/elieba/final_project/process_data.py
    """
    )

    load_task = BashOperator(
        task_id = 'Load_Data',
        bash_command="""
       spark-submit \
       --master spark://de-bootcamp.us-central1-b.c.alexy-de-bootcamp.internal:7077 \
       --jars /home/elieba/spark/lib/spark-3.5-bigquery-0.42.0.jar,/home/elieba/spark/lib/gcs-connector-hadoop3-2.2.5.jar \
       /home/elieba/final_project/load2BigQuery.py
    """
    )

    

    extract_task >> process_task >> load_task
