# Databricks notebook source
#dbutils.notebook.run("/mountExternalSource",timeout_seconds=60)
%run "./mountExternalSource"

# COMMAND ----------

ext_source = "/mnt/ext_source"
source_files_path = f"{ext_source}/source_files"
sparkDeltaTables = f"{ext_source}/sparkDeltaTables"

# COMMAND ----------

from pyspark.sql.functions import *
from delta.tables import *

class StoresSales:
    def readStoresSales(pbatch_date,pbatch_id=10) -> DataFrame:        
        filename = "StoresSales"      
        filepath = f"{source_files_path}/{filename}_{pbatch_date}.csv"
        df = spark.read.format("csv").option("sep",",").option("header",True).option("inferSchema",True).load(filepath)
        df = df.select(
            col("uuid").cast("int").alias("uuid"),
            col("Country").cast("string").alias("Country"),
            col("Item Type").cast("string").alias("Item_Type"),
            col("Sales Channel").cast("string").alias("Sales_Channel"),
            col("Order Priority").cast("string").alias("Order_Priority"),
            to_date(col("Order Date"),"yyyy-MM-dd").alias("Order_Date"),
            ((year(to_date(col("Order Date"),"yyyy-MM-dd"))*10000)  + (month(to_date(col("Order Date"),"yyyy-MM-dd"))*100) + (dayofmonth(to_date(col("Order Date"),"yyyy-MM-dd")))).alias("OrderDate"),            
            col("Region").cast("string").alias("Region"),
            to_date(col("Ship Date"),"yyyy-MM-dd").alias("Ship_Date"),
            col("Units Sold").cast("int").alias("Units_Sold"),
            col("Unit Price").cast("double").alias("Units_Price"),
            col("Unit Cost").cast("double").alias("Units_Cost"),
            col("Total Revenue").cast("double").alias("Total_Revenue"),
            col("Total Cost").cast("double").alias("Total_Cost"),
            col("Total Profit").cast("double").alias("Total_Profit"),
            to_date(current_date(),"yyyy-MM-dd").alias("Start_Date"),
            to_date(current_date(),"yyyy-MM-dd").alias("End_Date"),
            lit('Y').cast("string").alias("isActive"),
            lit(pbatch_id).cast("int").alias("Batch_id"),
            current_timestamp().cast("timestamp").alias("Update_Ts")
        )
        return df
    
    def createPartitionedTableStoresSales():    
        DatabaseName = "default"
        TableName = "StoresSales"
        TableName = f"{DatabaseName}.{TableName}"
        TablePath = f"{sparkDeltaTables}/{TableName}/"
        
        DeltaTable.createIfNotExists(spark) \
        .tableName(TableName) \
        .addColumn("uuid","int") \
        .addColumn("Country","STRING") \
        .addColumn("Item_Type","STRING") \
        .addColumn("Sales_Channel","STRING") \
        .addColumn("Order_Priority","STRING") \
        .addColumn("Order_Date","Date") \
        .addColumn("OrderDate","int") \
        .addColumn("Region","STRING") \
        .addColumn("Ship_Date","Date") \
        .addColumn("Units_Sold","int") \
        .addColumn("Units_Price","double") \
        .addColumn("Units_Cost","double") \
        .addColumn("Total_Revenue","double")\
        .addColumn("Total_Cost","double")\
        .addColumn("Total_Profit","double")\
        .addColumn("Start_Date","Date")\
        .addColumn("End_Date","Date")\
        .addColumn("isActive","STRING")\
        .addColumn("Batch_id","INT")\
        .addColumn("Update_Ts","timestamp")\
        .property("Desciption","This Table is for StoresSales") \
        .partitionedBy("OrderDate") \
        .location(TablePath) \
        .execute()

    def createDeltaTableStoresSales():    
        DatabaseName = "default"
        TableName = "NonP_StoresSales"
        TableName = f"{DatabaseName}.{TableName}"
        TablePath = f"{sparkDeltaTables}/{TableName}/"
        
        DeltaTable.createIfNotExists(spark) \
        .tableName(TableName) \
        .addColumn("uuid","int") \
        .addColumn("Country","STRING") \
        .addColumn("Item_Type","STRING") \
        .addColumn("Sales_Channel","STRING") \
        .addColumn("Order_Priority","STRING") \
        .addColumn("Order_Date","Date") \
        .addColumn("OrderDate","int") \
        .addColumn("Region","STRING") \
        .addColumn("Ship_Date","Date") \
        .addColumn("Units_Sold","int") \
        .addColumn("Units_Price","double") \
        .addColumn("Units_Cost","double") \
        .addColumn("Total_Revenue","double")\
        .addColumn("Total_Cost","double")\
        .addColumn("Total_Profit","double")\
        .addColumn("Start_Date","Date")\
        .addColumn("End_Date","Date")\
        .addColumn("isActive","STRING")\
        .addColumn("Batch_id","INT")\
        .addColumn("Update_Ts","timestamp")\
        .property("Desciption","This Table is for StoresSales") \
        .location(TablePath) \
        .execute()

    def dropTableStoresSales():    
        DatabaseName = "default"
        TableName = "StoresSales"
        TableName = f"{DatabaseName}.{TableName}"
        TablePath = f"{sparkDeltaTables}/{TableName}/"
        spark.sql(f"DROP TABLE IF EXISTS {TableName}  ")
        
    def loadDeltaLakeTableStoresSales(pdate):
        #srcdfStoresSales = StoresSales.readStoresSales(pdate)
        #spark.sql("Delete from default.StoresSales")
        #Target Data - Delta Table
        #tgtdfStoresSales = DeltaTable.forName(spark,"default.StoresSales").toDF()
        #tgtdfStoresSales = tgtdfStoresSales.filter("isActive = 'Y'")
        #Ignore Already Existing Records
        #newStoresSalesDF = srcdfStoresSales.join(tgtdfStoresSales, ((srcdfStoresSales.uuid == tgtdfStoresSales.uuid) ), "left").select(srcdfStoresSales["*"],tgtdfStoresSales.Batch_id.alias("Target_Batch_id"))
        #newStoresSalesDF = newStoresSalesDF.filter(col("Target_Batch_id").isNull()).drop("Target_Batch_id")
        #Write into the Delta Lake
        newStoresSalesDF = srcdfStoresSales
        newStoresSalesDF.write.format("delta").mode("append").saveAsTable("default.NonP_StoresSales")
        #newStoresSalesDF.write.format("delta").partitionBy("OrderDate").mode("append").saveAsTable("default.StoresSales2")

# COMMAND ----------

#StoresSales.createDeltaTableStoresSales()
StoresSales.createDeltaTableStoresSales()

# COMMAND ----------

#.partitionedBy("OrderDate") \
#StoresSales.dropTableStoresSales()
#StoresSales.createPartitionedTableStoresSales()
#StoresSales.createDeltaTableStoresSales()
StoresSales.loadDeltaLakeTableStoresSales('20230101')

# COMMAND ----------

srcdfStoresSales = StoresSales.readStoresSales('20230101',32)
StoresSales.loadDeltaLakeTableStoresSales('20230101')



# COMMAND ----------

# MAGIC %sql 
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled = false

# COMMAND ----------

# MAGIC %sql VACUUM default.NonP_StoresSales RETAIN 0 hours

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/ext_source/sparkDeltaTables/default.NonP_StoresSales/

# COMMAND ----------

# MAGIC %sql 
# MAGIC Describe History default.StoresSales

# COMMAND ----------

dbutils.fs.rm("dbfs:/mnt/ext_source/sparkDeltaTables/default.StoresSales",True)


# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from NonP_StoresSales

# COMMAND ----------

# MAGIC %sql
# MAGIC Select
# MAGIC count(Distinct Batch_id) as TotalRun_count,
# MAGIC count(1) as All_rows_count,
# MAGIC count(Distinct uuid) as uuid_count,
# MAGIC count(Distinct Country) as Country_count,
# MAGIC count(Distinct Item_Type) as Item_Type_count,
# MAGIC count(Distinct Sales_Channel) as Item_Type_count,
# MAGIC count(Distinct Order_Priority) as Order_Priority_count,
# MAGIC count(Distinct OrderDate) as OrderDate_count,
# MAGIC count(Distinct Region) as Region_count,
# MAGIC count(Distinct Ship_Date) as Ship_Date_count
# MAGIC 
# MAGIC from NonP_StoresSales