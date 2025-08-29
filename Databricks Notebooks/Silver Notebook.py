# Databricks notebook source
# Retreive the task value from the previous task (bronze)
bronze_output = dbutils.jobs.taskValues.get(taskKey = "Bronze", key = "bronze_output")

# Access individual variables
start_date = bronze_output.get("start_date", "")
bronze_adls = bronze_output.get("bronze_adls", "")
silver_adls = bronze_output.get("silver_adls", "")

print(f"Start Date : {start_date}, Bronze ADLS : {bronze_adls}")

# COMMAND ----------

from pyspark.sql.functions import col, isnull, when
from pyspark.sql.types import TimestampType
from datetime import date, timedelta

# COMMAND ----------

# Load the JSON data into a Spark DataFrame
df = spark.read.option("multiline", "true").json(f"{bronze_adls}/{start_date}_earthquake_data.json")

# COMMAND ----------

# Reshape Earthquake data
df = (
    df.select(
        'id',
        col('geometry.coordinates').getItem(0).alias('longitude'),
        col('geometry.coordinates').getItem(1).alias('latitude'),
        col('geometry.coordinates').getItem(2).alias('elevation'),
        col('properties.title').alias('title'),
        col('properties.place').alias('place_description'),
        col('properties.sig').alias('sig'),
        col('properties.mag').alias('mag'),
        col('properties.magType').alias('magtype'),
        col('properties.time').alias('time'),
        col('properties.updated').alias('updated')
    )
)

# COMMAND ----------

# Validate data: Check for missing or null values
df = (
    df
    .withColumn('longitude', when(isnull('longitude'), 0).otherwise('longitude'))
    .withColumn('latitude', when(isnull('latitude'), 0).otherwise('latitude'))
    .withColumn('time', when(isnull(col('time')), 0).otherwise(col('time')))
)

# COMMAND ----------

# Convert 'time' and 'updated' to timestamp from Unix time
df = (
    df.withColumn('time', (col('time')/1000).cast(TimestampType()))
    .withColumn('updated', (col('updated')/1000).cast(TimestampType()))
)

# COMMAND ----------

silver_output_path = f"{silver_adls}/{start_date}_earthquake_events_silver/"

# COMMAND ----------

df.write.mode('append').parquet(silver_output_path)

# COMMAND ----------

# Return the dictionary directly
dbutils.jobs.taskValues.set(key = "silver_output", value = silver_output_path)