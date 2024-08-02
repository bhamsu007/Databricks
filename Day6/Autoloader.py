# Databricks notebook source
(spark
.readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.inferColumnTypes",True)
.option("cloudFiles.schemaLocation","dbfs:/mnt/hexawaredatabricks/raw/schemalocation/SHUBHAM/autoloader")
.load("dbfs:/mnt/hexawaredatabricks/raw/stream_in/")
.writeStream
.option("checkpointLocation","dbfs:/mnt/hexawaredatabricks/raw/checkpoint/Shubham/autoloader")
.trigger(once=True)
.table("databricksworkspace.bronze.Autoloader")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricksworkspace.bronze.Autoloader

# COMMAND ----------

# MAGIC %md
# MAGIC **Mode	- Behavior on reading new column**
# MAGIC
# MAGIC **addNewColumns (default)**	- Stream fails. New columns are added to the schema. Existing columns do not evolve data types.
# MAGIC
# MAGIC **rescue**	- Schema is never evolved and stream does not fail due to schema changes. All new columns are recorded in the rescued data column.
# MAGIC
# MAGIC **failOnNewColumns**	- Stream fails. Stream does not restart unless the provided schema is updated, or the offending data file is removed.
# MAGIC
# MAGIC **none** -	Does not evolve the schema, new columns are ignored, and data is not rescued unless the rescuedDataColumn option is set. Stream does not fail due to schema changes.

# COMMAND ----------

(spark
.readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.inferColumnTypes",True)
.option("cloudFiles.schemaEvolutionMode","rescue")
.option("cloudFiles.schemaLocation","dbfs:/mnt/hexawaredatabricks/raw/schemalocation/SHUBHAM/autoloader1")
.load("dbfs:/mnt/hexawaredatabricks/raw/stream_in/")
.writeStream
.option("checkpointLocation","dbfs:/mnt/hexawaredatabricks/raw/checkpoint/Shubham/autoloader1")
.option("mergeSchema",True)
.table("databricksworkspace.bronze.Autoloader")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricksworkspace.bronze.Autoloader where id=1

# COMMAND ----------


