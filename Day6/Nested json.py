# Databricks notebook source
# MAGIC %run /Workspace/Users/shubhamkr706@gmail.com/Training/Day5/Includes

# COMMAND ----------

df=spark.read.json(
"dbfs:/mnt/hexawaredatabricks/raw/input_files/adobe_sample.json"
)

# COMMAND ----------

df=spark.read.json(
"dbfs:/mnt/hexawaredatabricks/raw/input_files/adobe_sample.json",multiLine=True)

# COMMAND ----------

df.display()

# COMMAND ----------

(df.withColumn("batter",explode("batters.batter"))
 .withColumn("batter_id",col("batter.id"))
 .withColumn("batter_type",col("batter.type"))
.display())

# COMMAND ----------

df1=df.withColumn("batters",explode("batters.batter"))\
.withColumn("batters_id",col("batters.id"))\
.withColumn("batters_type",col("batters.type"))\
.drop("batters")\
.withColumn("topping",explode("topping"))\
.withColumn("topping_id",col("topping.id"))\
.withColumn("topping_type",col("topping.type"))\
.drop("topping")

# COMMAND ----------

df1.display()

# COMMAND ----------

df1.createOrReplaceTempView("adobe")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from adobe

# COMMAND ----------


