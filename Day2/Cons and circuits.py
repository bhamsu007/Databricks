# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/bhamsustorage/training/dataset/

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog databricksworkspace;
# MAGIC create schema formula1
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create table formula1.constructors as
# MAGIC select * from json.`dbfs:/mnt/bhamsustorage/training/dataset/constructors.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricksworkspace.formula1.constructors

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from csv.`dbfs:/mnt/bhamsustorage/training/dataset/circuits.csv`
# MAGIC
# MAGIC --CTAS doesnt work with csv format.

# COMMAND ----------

df=spark.read.csv("dbfs:/mnt/bhamsustorage/training/dataset/circuits.csv",header=True,inferSchema=True)
#df=spark.read.option("header",True).option("inferSchema",True).csv("path")

# COMMAND ----------

#df.show()
df.display()

# COMMAND ----------

df.write.saveAsTable("databricksworkspace.formula1.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1.Circuits

# COMMAND ----------

from pyspark.sql.functions import *
df.select(col("circuitId"),"Name",df["location"]).show()

# COMMAND ----------

df.select(concat("location",lit(" "),"country")).display()

# COMMAND ----------

df.withColumnRenamed("circuitid","circuit_id").display()

# COMMAND ----------

df.withColumn("Ingestion_Date",current_date()).display()

# COMMAND ----------

(df.
withColumn("Ingestion_Date",current_date()).
withColumn("Input_file",input_file_name()).
display())

# COMMAND ----------

#drop url
df.\
withColumn("Ingestion_Date",current_date()).\
withColumn("Input_file",input_file_name()).\
drop("url").\
display()

# COMMAND ----------


