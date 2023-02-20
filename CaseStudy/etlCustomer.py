# Databricks notebook source
from pyspark.sql.functions import *
from delta.tables import *

class Customer:    
    def readCustomer(pbatch_date) -> DataFrame:    
        filename = "Customer"
        #defined the schema for the file
        schemaCustomer = StructType(
        [StructField("Customer_id",IntegerType(),True),
        StructField("Customer_Name",StringType(),True),
        StructField("Customer_Phone",StringType(),True),
        StructField("Customer_Date",StringType(),True),
        StructField("Customer_MaritalStatus",StringType(),True),
        StructField("Customer_Points",IntegerType(),True) ]) 

        filepath = f"{source_files_path}/{filename}_{pbatch_date}.csv"    
        df = spark.read.format("csv").option("sep",",").option("header",True).schema(schemaCustomer).load(filepath)

        #Transformation Applied
        df = df.withColumn('Customer_Date',to_date(col("Customer_Date"),"dd/MM/yyyy").alias("Customer_Date")) \
               .withColumn("Customer_MaritalStatus", when(df.Customer_MaritalStatus == "M","Married").when(df.Customer_MaritalStatus == "S","Single").when(df.Customer_MaritalStatus.isNull() ,"")      .otherwise(df.Customer_MaritalStatus)) \
        .withColumn('Customer_Type',when(df.Customer_Points < 200,"Standard Customer").when( ((df.Customer_Points >= 200) & (df.Customer_Points <= 500)),"Silver Customer").when( ((df.Customer_Points > 500) ),"Gold Customer") ) \
        .withColumn('Insert_Ts',current_timestamp())
        return df
