# Databricks notebook source
# DBTITLE 1,Imports
# MAGIC %run /Workspace/Users/shubhamkr706@gmail.com/Training/Day2/Includes

# COMMAND ----------

# DBTITLE 1,Read
df=spark.read.csv(input_path,header=True,inferSchema=True)

# COMMAND ----------

# DBTITLE 1,Transform and write

df.withColumnRenamed("raceId","race_id").\
  withColumnRenamed("circuitId","circuit_id").\
    withColumn("processing_date",current_timestamp()).\
      withColumn("File_name",input_file_name()).\
        drop("url").write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.Processed_Races")

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table formula1.Processed_Races

# COMMAND ----------

# DBTITLE 1,validating data
# MAGIC %sql
# MAGIC select * from formula1.Processed_Races

# COMMAND ----------

# MAGIC %sql
# MAGIC select name,count(*) as count from formula1.Processed_Races group by name order by count desc

# COMMAND ----------

_sqldf.show()

# COMMAND ----------


