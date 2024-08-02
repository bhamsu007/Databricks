# Databricks notebook source
# MAGIC %md
# MAGIC Step1: Read the csv
# MAGIC  
# MAGIC Step 2: Transformation
# MAGIC - rename raceId to race_id and circuitId to circuit_id
# MAGIC - new column that should contain current timestamp
# MAGIC - new column that should contain path
# MAGIC - drop url column
# MAGIC  
# MAGIC step3: save it to table

# COMMAND ----------

df=spark.read.csv("dbfs:/mnt/bhamsustorage/training/dataset/rac*",header=True,inferSchema=True)

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import *
df.withColumnRenamed("raceId","race_id").\
  withColumnRenamed("circuitId","circuit_id").\
    withColumn("processing_date",current_timestamp()).\
      withColumn("File_name",input_file_name()).\
        drop("url").write.saveAsTable("formula1.Processed_Races")

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table formula1.Processed_Races

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1.Processed_Races

# COMMAND ----------


