# Databricks notebook source
# Retreive the task value from the previous task (bronze & silver)
bronze_output = dbutils.jobs.taskValues.get(taskKey = "Bronze", key = "bronze_output")
silver_data = dbutils.jobs.taskValues.get(taskKey = "Silver", key = "silver_output")

# Acceess individual variables
start_date = bronze_output["start_date"]
silver_adls = bronze_output["silver_adls"]
gold_adls = bronze_output["gold_adls"]

gold_output_path = f"{gold_adls}/{start_date}_earthquake_events_gold/"

# COMMAND ----------

from pyspark.sql.functions import when, col, udf
from pyspark.sql.types import StringType
# Ensure the below library is installed on your cluster
import reverse_geocoder as rg
from datetime import date, timedelta

# COMMAND ----------

df = spark.read.parquet(silver_data).filter(col('time') > start_date) 

# COMMAND ----------

df = df.limit(10) #added to speed up processing as during testing it was proving a bottleneck
# The probleme is caused by the bython UDF (reverse_geocoder) being a bottleneck due to its non-parallel nature and high computational cost per task

# COMMAND ----------

def get_country_code(lat, lon):
    try:
        coordinates = (float(lat), float(lon))
        result = rg.search(coordinates)[0].get('cc')
        print(f"Processed coordinates: {coordinates} -> {result}")
    except Exception as e:
        print(f"Error processing coordinates: {lat}, {lon} -> {str(e)}")
    return None

# COMMAND ----------

get_country_code_udf = udf(get_country_code, StringType())

# COMMAND ----------

df_with_location = df.withColumn('country_code', get_country_code_udf(col('latitude'), col('longitude')))

# COMMAND ----------

df_with_location_sig_class = df_with_location.withColumn('sig_class', when(col('sig') < 100, 'Low')
                                                         .when((col('sig') >= 100) & (col('sig') < 500), 'Mederate')
                                                         .otherwise('Hight'))

# COMMAND ----------

df_with_location_sig_class.write.mode('append').parquet(gold_output_path)