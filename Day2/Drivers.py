# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/bhamsustorage/training/dataset/

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended formula1.circuits

# COMMAND ----------

# MAGIC %sql
# MAGIC create table formula1.drivers as
# MAGIC select * from json.`dbfs:/mnt/bhamsustorage/training/dataset/drivers.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1.drivers

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table formula1.drivers

# COMMAND ----------

df_drivers=spark.read.json("dbfs:/mnt/bhamsustorage/training/dataset/drivers.json")

# COMMAND ----------

df_drivers.show()

# COMMAND ----------


