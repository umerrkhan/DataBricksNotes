# Databricks notebook source
from pyspark.sql.functions import *
from delta.tables import *

class Sales:
    def readSales(pbatch_date) -> DataFrame:    
        filename = "Sales"
        #defined the schema for the file
        schemaSales = StructType(
        [StructField("Order_id",IntegerType(),True),
        StructField("Product_id",IntegerType(),True),
        StructField("Customer_id",IntegerType(),True),
        StructField("Quantity",IntegerType(),True),
        StructField("Sales_Date",StringType(),True)])        

        filepath = f"{source_files_path}/{filename}_{pbatch_date}.csv"
        df = spark.read.format("csv").option("sep",",").option("header",True).schema(schemaSales).load(filepath)

        #Transformation Applied
        df = df.withColumn('Sales_Date',to_date(col("Sales_Date"),"dd/MM/yyyy").alias("Sales_Date"))\
        .withColumn('Insert_Ts',current_timestamp())
        return df
    
    