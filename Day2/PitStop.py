# Databricks notebook source
df=spark.read.text("dbfs:/mnt/bhamsustorage/training/dataset/pit_stops.json")

# COMMAND ----------

df.show(truncate=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from json.`dbfs:/mnt/bhamsustorage/training/dataset/pit_stops.json`

# COMMAND ----------

df=spark.read.option("multiline",True).json("dbfs:/mnt/bhamsustorage/training/dataset/pit_stops.json")

# COMMAND ----------

df.show()

# COMMAND ----------

df.write.saveAsTable("formula1.PitStop")

# COMMAND ----------


