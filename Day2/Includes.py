# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

input_path="dbfs:/mnt/bhamsustorage/training/dataset/races.csv"

# COMMAND ----------

catalog="databricksworkspace";
schema="formula1"

# COMMAND ----------


