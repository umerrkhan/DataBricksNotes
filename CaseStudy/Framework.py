# Databricks notebook source
# MAGIC %run "./projectvariables"

# COMMAND ----------

#Temporary comment
#%run "./mountExternalSource"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### ETL Framework

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from delta.tables import *

class ETLFramework:     

    def createTablefrw_BatchDate():        
        DatabaseName = "default"
        TableName = "frw_BatchDate"
        TableName = f"{DatabaseName}.{TableName}"
        TablePath = f"{sparkDeltaTables}/{TableName}/"

        DeltaTable.createIfNotExists(spark) \
        .tableName(TableName) \
        .addColumn("Batch_key","int") \
        .addColumn("Batch_Name","STRING") \
        .addColumn("Batch_Date","Date") \
        .addColumn("Batch_StartDate","timestamp") \
        .addColumn("Batch_EndDate","timestamp") \
        .addColumn("Batch_Status","STRING") \
        .addColumn("Batch_Id","INT") \
        .addColumn("Update_ts","timestamp") \
        .property("Desciption","This Table is for Metadata Table") \
        .location(TablePath) \
        .execute()
        #spark.sql(f"desc formatted {TableName}").show(truncate = False)
        display(spark.sql(f"Select * from {TableName}"))

    def optimizeTablefrw_BatchDate():
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled",False)
        spark.conf.get("spark.databricks.delta.retentionDurationCheck.enabled")
        spark.sql("OPTIMIZE default.frw_BatchDate ZORDER BY Batch_key")        
        spark.sql("Vacuum default.frw_BatchDate Retain 0 Hours")
        
    def restoreTablefrw_BatchDate(versionNumber=1):
        batchInstance = DeltaTable.forName(spark,"default.frw_BatchDate")        
        batchInstanceDeltaTable.restoreToVersion(versionNumber)
        display(batchInstance.history())
        
    def dropTablefrw_BatchDate(versionNumber=1):
        spark.sql("DROP TABLE IF EXISTS frw_BatchDate")
        
    def countRowsfrw_BatchDate():
        spark.sql("Select count(1) from frw_BatchDate").count

    def getBatchDetails(pbatch_key = None):
        print(f"batch_key = {pbatch_key}")        
        if pbatch_key is None:
            spark.sql(f"Select * from default.frw_BatchDate").show()
        else:
            spark.sql(f"Select * from default.frw_BatchDate where batch_key = {pbatch_key}").show(1)
    
    def getTableHistory(tablename="frw_BatchDate"):
        DatabaseName = "default"        
        TableName = f"{DatabaseName}.{tablename}"
        batchInstance = DeltaTable.forName(spark,TableName) 
        display(batchInstance.history())
        
    def initfrw_BatchDate(Batch_Key=100,Batch_Name='NewBatchRegistered',Batch_Date='2023-01-01'):    
        DatabaseName = "default"
        TableName = "frw_BatchDate"
        TableName = f"{DatabaseName}.{TableName}"
        TablePath = f"{sparkDeltaTables}/{TableName}/"
        
        rowsCount = spark.sql(f"Select count(1) as cnt from {TableName} where batch_key = {Batch_Key}").collect()[0][0]
        
        if rowsCount >= 1:
            print("Batch Key is already Registerd")
        else :
            print("Registering the Batch")
            data = [(Batch_Key,Batch_Name,Batch_Date)]
            schema = ['Batch_key','Batch_Name','Batch_Date']
            df = spark.createDataFrame(data,schema=schema)

            df = df.withColumn('Batch_StartDate',current_timestamp()) \
            .withColumn('Batch_EndDate',current_timestamp()) \
            .withColumn('Batch_Status',lit('Closed')) \
            .withColumn('Batch_Id',lit(1)) \
            .withColumn('Update_ts',current_timestamp()).select(
                col('Batch_key').cast("int"),
                col('Batch_Name').cast("string"),
                col('Batch_Date').cast("Date"),
                col('Batch_StartDate').cast("timestamp"),
                col('Batch_EndDate').cast("timestamp"),
                col('Batch_Status').cast("string"),
                col('Batch_Id').cast("int"),
                col('Update_ts').cast("timestamp")    
            )
            df.write.format("delta").mode("append").saveAsTable(TableName)
            ETLFramework.getBatchDetails(Batch_Key)
        
    
    
    def setBatch(pbatch_key,pbatch_status):
        batchInstance = DeltaTable.forName(spark,"default.frw_BatchDate") 

        if pbatch_status == "Open":        
            batchInstance.update(
                condition = "batch_key = 10 and batch_status = 'Closed' ",
                set = { "batch_status" : lit("Open"), 
                        "Batch_Date" : date_add('Batch_Date', 1), 
                       "batch_Id" : col("batch_Id")+1,
                       "Batch_StartDate" : current_timestamp() } )

        if pbatch_status == "Closed":
            batchInstance.update(
                condition = "batch_key = 10 and batch_status = 'Open' ",
                set = {"batch_status" : lit("Closed"), "Batch_EndDate" : current_timestamp() } )


        batchDF = batchInstance.toDF()
        batchDF = batchDF.filter(f"batch_key = {pbatch_key}")
        batchDF = batchDF.withColumn("BatchDateFormat",((year("Batch_Date")*10000)+(month("Batch_Date")*100)+(dayofmonth("Batch_Date")))) \
        .withColumn("Batch_Id",col("Batch_Id"))\
        .select("BatchDateFormat","Batch_Id")
        BatchDateFormat = batchDF.collect()[0][0]
        Batch_Id = batchDF.collect()[0][1]
        #print(BatchDateFormat)
        #print(Batch_Id)
        return BatchDateFormat,Batch_Id
    
    def getBatch(pbatch_key = None):
        
        if pbatch_key is not None:            
            batchInstance = DeltaTable.forName(spark,"default.frw_BatchDate")
            batchDF = batchInstance.toDF()        
            batchDF = batchDF.filter(f"batch_key = {pbatch_key}")
            batchDF = batchDF.withColumn("BatchDateFormat",((year("Batch_Date")*10000)+(month("Batch_Date")*100)+(dayofmonth("Batch_Date")))) \
            .withColumn("Batch_Id",col("Batch_Id"))\
            .select("BatchDateFormat","Batch_Id")        
            
            if batchDF.count() > 0:                
                BatchDateFormat = batchDF.collect()[0][0]
                Batch_Id = batchDF.collect()[0][1]
            else:
                print(f"No Batch registered against this Batch key {pbatch_key}")
                BatchDateFormat = -1
                Batch_Id = -1                
        else:
            BatchDateFormat = -1        
            Batch_Id = -1
        return BatchDateFormat,Batch_Id                         
        
    
    def addDefaultMetadataColumns(pDataFrame,pbatch_id=7777,startdatecolvalue=None):        
        if startdatecolvalue is None:
            pDataFrame = pDataFrame.withColumn("StartDate",current_date())
            print("default")
        else: 
            print("value")
            pDataFrame = pDataFrame.withColumn("StartDate",col(startdatecolvalue))          
        
        pDataFrame = pDataFrame.withColumn('EndDate',to_date(lit('9999-12-31'),"yyyy-MM-dd")) \
        .withColumn('isActive',lit('Y')) \
        .withColumn('batch_id',lit(pbatch_id)) \
        .withColumn('Update_Ts',current_timestamp())     
        return pDataFrame
    
    def createTablefrw_TransformColumns():
        DatabaseName = "default"
        TableName = "frw_TransformColumns"
        TableName = f"{DatabaseName}.{TableName}"
        TablePath = f"{sparkDeltaTables}/{TableName}/"
        DeltaTable.createIfNotExists(spark) \
        .tableName(TableName) \
        .addColumn("Target_DataBase","STRING") \
        .addColumn("Target_TableName","STRING") \
        .addColumn("Target_ColumnName","STRING") \
        .addColumn("isColumnPK","STRING") \
        .addColumn("ColumnSortOrder","int") \
        .property("Desciption","This Table is for Metadata Table") \
        .location(TablePath) \
        .execute()
        
        
    def getfrw_TransformColumns(pbatch_key = None):
        
        if pbatch_key is not None:            
            batchInstance = DeltaTable.forName(spark,"default.frw_BatchDate")
            batchDF = batchInstance.toDF()        
            batchDF = batchDF.filter(f"batch_key = {pbatch_key}")
            batchDF = batchDF.withColumn("BatchDateFormat",((year("Batch_Date")*10000)+(month("Batch_Date")*100)+(dayofmonth("Batch_Date")))) \
            .withColumn("Batch_Id",col("Batch_Id"))\
            .select("BatchDateFormat","Batch_Id")        
            
            if batchDF.count() > 0:                
                BatchDateFormat = batchDF.collect()[0][0]
                Batch_Id = batchDF.collect()[0][1]
            else:
                print(f"No Batch registered against this Batch key {pbatch_key}")
                BatchDateFormat = -1
                Batch_Id = -1                
        else:
            BatchDateFormat = -1        
            Batch_Id = -1
        return BatchDateFormat,Batch_Id
    
    def registerTransformColumns(Target_DataBase,Target_TableName,Target_ColumnName,isColumnPK,ColumnSortOrder):             
        DatabaseName = "default"
        TableName = "frw_TransformColumns"
        TableName = f"{DatabaseName}.{TableName}"
        data = [(Target_DataBase,Target_TableName,Target_ColumnName,isColumnPK,ColumnSortOrder)]
        schema = ["Target_DataBase","Target_TableName","Target_ColumnName","isColumnPK","ColumnSortOrder"]
        df = spark.createDataFrame(data,schema=schema)
        df = df.select(
                        col('Target_DataBase').cast("string"),
                        col('Target_TableName').cast("string"),
                        col('Target_ColumnName').cast("string"),
                        col('isColumnPK').cast("string"),    
                        col('ColumnSortOrder').cast("int")
        )
        stmt = f"Select count(1) as cnt from {TableName} where Target_DataBase= '{Target_DataBase}' AND Target_TableName = '{Target_TableName}' AND Target_ColumnName = '{Target_ColumnName}' "
        rowsCount = spark.sql(stmt).collect()[0][0]

        if rowsCount >= 1:
            print("Column is already Registerd")
        else:
            df.write.format("delta").mode("append").saveAsTable(TableName)
        stmt = f"Select * from {TableName} where Target_DataBase= '{Target_DataBase}' AND Target_TableName = '{Target_TableName}' order by isColumnPK,ColumnSortOrder ASC "
        display(spark.sql(stmt))



#spark.sql(f"desc formatted {TableName}").show(truncate = False)

    
    
    

# COMMAND ----------

ETLFramework.createTablefrw_BatchDate()
ETLFramework.initfrw_BatchDate(Batch_Key=10,Batch_Name='Sales',Batch_Date='2022-12-31')
ETLFramework.registerTransformColumns("default","Product","Product_Name","N","1")