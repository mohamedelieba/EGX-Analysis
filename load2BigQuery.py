from pyspark.sql import SparkSession

# Initialize Spark session with BigQuery connector
spark = SparkSession.builder \
    .master('spark://de-bootcamp.us-central1-b.c.alexy-de-bootcamp.internal:7077') \
    .appName('test') \
    .config("spark.jars", "/home/elieba/spark/lib/spark-3.5-bigquery-0.42.0.jar,/home/elieba/spark/lib/gcs-connector-hadoop3-2.2.5.jar") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/home/elieba/.gc/alexy-de-bootcamp.json") \
    .getOrCreate()

# Set BigQuery credentials
spark.conf.set("credentialsFile", "/home/elieba/.gc/alexy-de-bootcamp.json")

spark.conf.set("temporaryGcsBucket",  "dataproc-temp-us-central1-486633816792-zeialixk")

# Read processed data from Parquet files
processed_data = spark.read.format("parquet").load('/home/elieba/data/processed_data/*')

# Write data to BigQuery
processed_data.write.format('bigquery') \
    .option('table', 'alexy-de-bootcamp.EGX_dataset.basic_info') \
    .option("partitionField", "Company") \
    .option("clusteredFields", "Sector")\
    .mode('overwrite')\
    .save()

# Stop the Spark session
spark.stop()