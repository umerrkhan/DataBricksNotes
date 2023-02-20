# Databricks notebook source
# MAGIC %run "./Framework"

# COMMAND ----------

# MAGIC %run "./etlProduct"

# COMMAND ----------


ProcessBatchDate,Batch_Id = ETLFramework.setBatch(10,"Open")
print(ProcessBatchDate)
print(Batch_Id)
ProcessBatchDate,Batch_Id = ETLFramework.setBatch(10,"Closed")




# COMMAND ----------

ETLFramework.getBatch(10)