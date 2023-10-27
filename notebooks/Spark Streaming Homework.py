# Databricks notebook source
# MAGIC %md
# MAGIC # Spark Streaming Homework

# COMMAND ----------

# Connect to Storage Account
spark.conf.set(
    "fs.azure.account.key.stmdsparkstrmswesteurope.dfs.core.windows.net",
    dbutils.secrets.get(scope="hw", key="storage_account_key"))

# COMMAND ----------

data_path = "abfss://data@stmdsparkstrmswesteurope.dfs.core.windows.net/hotel-weather"
checkpoint_path = "abfss://data@stmdsparkstrmswesteurope.dfs.core.windows.net/checkpoints"

# COMMAND ----------

# stream
from pyspark.sql.functions import *

schema = (
    StructType()
    .add("address", "string")
    .add("avg_tmpr_c", "double")
    .add("avg_tmpr_f", "double")
    .add("city", "string")
    .add("country", "string")    
    .add("geoHash", "string")
    .add("id", "string")
    .add("latitude", "double")
    .add("longitude", "double")
    .add("name", "string")
    .add("wthr_date", "string")
    .add("year", "string")
    .add("month", "string")
    .add("day", "string")
)

streaming_data = spark.readStream \
                    .format("cloudFiles") \
                    .schema(schema) \
                    .option("cloudFiles.format", "parquet") \
                    .option("cloudFiles.partitionColumns", "year, month, day") \
                    .load(data_path)

display(streaming_data)


# COMMAND ----------

processed_data = (
    streaming_data.groupBy("city", "wthr_date")
    .agg(
        approx_count_distinct("id").alias("distinct_hotels"),
        round(avg("avg_tmpr_c"), 2).alias("avg_temp"),
        max("avg_tmpr_c").alias("max_temp"),
        min("avg_tmpr_c").alias("min_temp")
    )
)
display(processed_data)

# COMMAND ----------

query = processed_data \
    .writeStream \
    .format("delta") \
    .outputMode("complete") \
    .option("checkpointLocation", checkpoint_path) \
    .toTable("hotel_weather") 


# COMMAND ----------

topCities = spark.sql("SELECT city, max(distinct_hotels) as total_hotels  FROM hotel_weather GROUP BY city ORDER BY total_hotels DESC LIMIT 10")
display(topCities)

# COMMAND ----------

cityList = [row["city"] for row in topCities.collect()]

for city in cityList:
    cityData = spark.sql(f"SELECT city, wthr_date, distinct_hotels, avg_temp, max_temp, min_temp FROM hotel_weather WHERE city = '{city}'")
    
    display(cityData)
