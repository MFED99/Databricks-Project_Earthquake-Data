# Databricks notebook source
import requests
import json
from datetime import date, timedelta 

# COMMAND ----------

# Required each time the cluster is restarted wich should be only on the first notebook as they run in order

tiers = ["bronze", "silver", "gold"]
adls_paths = {tier: f"abfss://{tier}@storageearthquakeproject.dfs.core.windows.net/" for tier in tiers}

# Accessing Path
bronze_adls = adls_paths["bronze"]
silver_adls = adls_paths["silver"]
gold_adls = adls_paths["gold"]

dbutils.fs.ls(bronze_adls)
dbutils.fs.ls(silver_adls)
dbutils.fs.ls(gold_adls)

# COMMAND ----------

start_date = date.today() - timedelta(days=1)
end_date = date.today()

# COMMAND ----------

# Construct the API URL with start and end dates provided by Data Factory, formatted for geojson output.
url = f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={start_date}&endtime={end_date}"

try:
    # Make the GET request to fetch data
    response = requests.get(url)

    # Check if the request was successful
    response.raise_for_status()
    data = response.json().get('features', [])

    if not data:
        print("No data returned for the specified data range.")
    else:
        # Specify the ADLS path
        file_path = f"{bronze_adls}/{start_date}_earthquake_data.json"

        # Save the json data
        json_data = json.dumps(data, indent=4)
        dbutils.fs.put(file_path, json_data, overwrite=True)
        print(f"Data successfully savec to : {file_path}")
except requests.exceptions.RequestException as e:
    print(f"Error fetching data from API: {e}")

# COMMAND ----------

# Define your variable
output_data = {
    "start_date": start_date.isoformat(),
    "end_date": end_date.isoformat(),
    "bronze_adls": bronze_adls,
    "silver_adls": silver_adls,
    "gold_adls": gold_adls
}

# Return the dictionary directly
dbutils.jobs.taskValues.set(key = "bronze_output", value = output_data)