from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, FloatType
from pyspark.sql.functions import regexp_replace, col

spark = SparkSession.builder\
                    .master('spark://de-bootcamp.us-central1-b.c.alexy-de-bootcamp.internal:7077')\
                    .appName('test')\
                    .getOrCreate()


# Define schema for the CSV file
schema = StructType([
    StructField("Company", StringType(), True),
    StructField("MarketCap_B_EGP", StringType(), True),
    StructField("Price_EGP", StringType(), True),
    StructField("ChangePercent", StringType(), True),
    StructField("Volume_M_EGP", StringType(), True),
    StructField("RelVolume", StringType(), True),
    StructField("PE", StringType(), True),
    StructField("EPS_EGP", StringType(), True),
    StructField("EPSDilGrowth_YOY_Per", StringType(), True),
    StructField("DivYieldPercent", StringType(), True),
    StructField("Sector", StringType(), True),
    StructField("AnalystRating", StringType(), True)
])

# Read the CSV file
df = spark.read.csv("/home/elieba/data/EGX-30_Data.csv", header=False, schema=schema)

# Clean and process the data
df = df.withColumn("MarketCap_B_EGP", regexp_replace(col("MarketCap_B_EGP"), "[^0-9.]", "").cast(DoubleType())) \
       .withColumn("Price_EGP", regexp_replace(col("Price_EGP"), "[^0-9.]", "").cast(DoubleType())) \
       .withColumn("ChangePercent", regexp_replace(col("ChangePercent"), "[^0-9.-]", "").cast(FloatType())) \
       .withColumn("Volume_M_EGP", regexp_replace(col("Volume_M_EGP"), "[^0-9.]", "").cast(DoubleType())) \
       .withColumn("RelVolume", regexp_replace(col("RelVolume"), "[^0-9.]", "").cast(FloatType())) \
       .withColumn("PE", regexp_replace(col("PE"), "[^0-9.]", "").cast(FloatType())) \
       .withColumn("EPS_EGP", regexp_replace(col("EPS_EGP"), "[^0-9.]", "").cast(FloatType())) \
       .withColumn("EPSDilGrowth_YOY_Per", regexp_replace(col("EPSDilGrowth_YOY_Per"), "[^0-9.-]", "").cast(FloatType())) \
       .withColumn("DivYieldPercent", regexp_replace(col("DivYieldPercent"), "[^0-9.]", "").cast(FloatType()))

# write data to parquet file
df\
    .repartition(4)\
    .write.parquet('/home/elieba/data/processed_data', mode='overwrite')


