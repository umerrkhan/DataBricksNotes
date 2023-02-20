# Databricks notebook source
# MAGIC %md
# MAGIC ##### Product

# COMMAND ----------

# MAGIC %run "./projectvariables"

# COMMAND ----------

from pyspark.sql.functions import *
from delta.tables import *

class Product:
    def createTableDim_Product():    
        DatabaseName = "default"
        TableName = "Dim_Product"
        TableName = f"{DatabaseName}.{TableName}"
        TablePath = f"{sparkDeltaTables}/{TableName}/"
        DeltaTable.createIfNotExists(spark) \
        .tableName(TableName) \
        .addColumn("Product_id","int") \
        .addColumn("Product_Name","STRING") \
        .addColumn("Product_Category","STRING") \
        .addColumn("Product_Date","Date") \
        .addColumn("Product_Price","DECIMAL(3,2)") \
        .addColumn("Product_StartDate","Date") \
        .addColumn("Product_EndDate","Date") \
        .addColumn("isActive","STRING") \
        .addColumn("Batch_Id","INT") \
        .addColumn("Insert_Ts","timestamp") \
        .property("Desciption","This Table is for Product Dimenstion") \
        .location(TablePath) \
        .execute()        
        spark.sql(f"Select * from {TableName}").show(10)
    
    def readProduct(pbatch_date,pbatch_id) -> DataFrame:        
        filename = "Product"
        #defined the schema for the file
        schemaProduct = StructType(
        [StructField("Product_id",IntegerType(),True),
        StructField("Product_Name",StringType(),True),
        StructField("Product_Category",StringType(),True),
        StructField("Product_Date",DateType(),True),
        StructField("Product_Price",DecimalType(3,2),True)] )

        filepath = f"{source_files_path}/{filename}_{pbatch_date}.csv"
        dfProduct = spark.read.format("csv").option("sep",",").option("header",True).schema(schemaProduct).load(filepath)

        #Transformation Applied - Metadata Columns
        dfProduct = dfProduct.withColumn('Product_StartDate',current_date()) \
        .withColumn('Product_EndDate',to_date(lit('9999-12-31'),"yyyy-MM-dd")) \
        .withColumn('isActive',lit('Y')) \
        .withColumn('batch_id',lit(pbatch_id)) \
        .withColumn('Insert_Ts',current_timestamp())     
        return dfProduct
    
    def scd2onProduct(pbatch_date,pbatch_id):
        #Source Data
        srcdfProduct = readProduct(pbatch_date,pbatch_id)

        #Target Data - Delta Table
        tgtDimProductDF = DeltaTable.forName(spark,"default.dim_product").toDF()
        tgtDimProductDF = tgtDimProductDF.filter("isActive = 'Y'")

        # Finding New Records
        newProductsDF = srcdfProduct.join(tgtDimProductDF, ((srcdfProduct.Product_id == tgtDimProductDF.Product_id) ), "left").select(srcdfProduct["*"],tgtDimProductDF.Product_id.alias("Target_Product_id"))
        newProductsDF = newProductsDF.filter(col("Target_Product_id").isNull()).drop("Target_Product_id")

        # Finding Changed Records
        changedProductsDF = srcdfProduct.join(tgtDimProductDF, (
            (
                (srcdfProduct.Product_id == tgtDimProductDF.Product_id) & ('Y' == tgtDimProductDF.isActive)  

            ) &
            (
                (srcdfProduct.Product_Name != tgtDimProductDF.Product_Name) |
                (srcdfProduct.Product_Category != tgtDimProductDF.Product_Category) |
                (srcdfProduct.Product_StartDate != tgtDimProductDF.Product_StartDate) |
                (srcdfProduct.Product_Price != tgtDimProductDF.Product_Price) 
            )
        )).select(srcdfProduct["Product_id"],srcdfProduct["Product_Name"],srcdfProduct["Product_Category"],srcdfProduct["Product_Date"],srcdfProduct["Product_Price"],srcdfProduct["Product_StartDate"],srcdfProduct["Product_EndDate"],srcdfProduct["isActive"],srcdfProduct["batch_id"],srcdfProduct["Insert_Ts"])

        # Updating Closing Old Records
        changedProductsDF.createOrReplaceTempView("TempView_changedProducts")
        spark.sql("merge into default.dim_product as target using TempView_changedProducts as source \
        on source.Product_id = target.Product_id \
        WHEN MATCHED THEN UPDATE SET  isActive =  'N' , Product_EndDate = current_date() ")

        # Inserting New Records
        newProductsDF = changedProductsDF.union(newProductsDF)
        newProductsDF.write.format("delta").mode("append").saveAsTable("default.dim_product")

    

# COMMAND ----------

Product.createTableDim_Product()