# EGX Stock Market Data Pipeline

> ## To Access the final Dashboard
>  https://lookerstudio.google.com/reporting/256b3b51-734b-45bb-b474-383b6b7abc06


## Problem Description
The Egyptian Stock Exchange (EGX) lacks an easily accessible dataset that provides historical and real-time market trends for data analysis. This project aims to extract stock market data from TradingView, process it using Apache Spark, and load it into Google BigQuery for further analysis. The project consists of two main parts:

1. **Jupyter Notebook Data Analysis** - Exploring and analyzing historical EGX stock data.
2. **Airflow ETL Pipeline** - Automating data extraction, transformation, and loading (ETL) using Airflow and Spark.

---

## Project Structure
```
├── notebooks
│   ├── egx_data_analysis.ipynb
├── airflow_pipeline
│   ├── dags
│   │   ├── egx_pipeline.py
│   ├── scripts
│   │   ├── process_data.py
│   │   ├── load2BigQuery.py
│   ├── data
│   │   ├── EGX-30_Data.csv
```

---

## 1. Jupyter Notebook Analysis

In `egx_data_analysis.ipynb`, we perform exploratory data analysis (EDA) on historical stock data, including:
- Data cleaning and preprocessing
- Feature engineering
- Visualization of stock trends

### Key Steps:
- Load historical stock data from CSV files.
- Handle missing values and outliers.
- Visualize stock trends using Matplotlib and Seaborn.

---

## 2. Airflow ETL Pipeline

The ETL pipeline extracts, transforms, and loads EGX stock data into BigQuery.

### Workflow Overview:
1. **Extract**: Scrape stock market data from TradingView.
2. **Process**: Clean and preprocess data using PySpark.
3. **Load**: Store the processed data in Google BigQuery.

### Airflow DAG (`egx_pipeline.py`)
```python
with DAG("EGX-Pipeline",
          start_date=datetime(2025, 3, 7),
          schedule="@daily",
          catchup=False) as dag:
    
    extract_task = PythonOperator(
        task_id="Extract_Data",
        python_callable=extract_data,
    )

    process_task = BashOperator(
        task_id='Process_Data',
        bash_command="""
       spark-submit \
       --master spark://de-bootcamp.us-central1-b.c.alexy-de-bootcamp.internal:7077 \
       /home/elieba/final_project/process_data.py
    """
    )

    load_task = BashOperator(
        task_id='Load_Data',
        bash_command="""
       spark-submit \
       --master spark://de-bootcamp.us-central1-b.c.alexy-de-bootcamp.internal:7077 \
       --jars /home/elieba/spark/lib/spark-3.5-bigquery-0.42.0.jar \
       /home/elieba/final_project/load2BigQuery.py
    """
    )

    extract_task >> process_task >> load_task
```

---

### Data Processing (`process_data.py`)
- Reads scraped data from CSV.
- Cleans and transforms data using PySpark.
- Stores the output in Parquet format.

---

### Loading Data into BigQuery (`load2BigQuery.py`)
- Reads processed Parquet files.
- Uploads data into a BigQuery table with clustering and partitioning.

---

## Deployment & Execution

### Prerequisites:
- Apache Airflow
- Apache Spark
- Google Cloud SDK & BigQuery setup

### Running the Pipeline:
```bash
airflow dags trigger EGX-Pipeline
```

---

## Future Enhancements
- Implement real-time data streaming.
- Enhance data visualization dashboards.
- Expand coverage to additional stock exchanges.

---

This project provides a scalable ETL solution for EGX stock data analysis, combining Jupyter-based exploratory analysis with an automated Airflow pipeline. 🚀


