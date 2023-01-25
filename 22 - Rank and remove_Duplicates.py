import pyspark.sql.types as t
from pyspark.sql import Window
import pyspark.sql.functions as F
from datetime import datetime



sample_data = [
(100,"Mobile",5000,10,datetime.strptime('2023-01-01 10:01:00', '%Y-%m-%d %H:%M:%S')),
 (100,"Mobile",7000,7,datetime.strptime('2023-01-01 10:02:00', '%Y-%m-%d %H:%M:%S')),
(200,"Laptop",20000,7,datetime.strptime('2023-01-01 10:01:00', '%Y-%m-%d %H:%M:%S')),
(200,"Laptop",25000,7,datetime.strptime('2023-01-01 10:02:00', '%Y-%m-%d %H:%M:%S')),
(200,"Laptop",22000,7,datetime.strptime('2023-01-01 10:03:00', '%Y-%m-%d %H:%M:%S'))

]



defSchema = StructType([
StructField("Product_id",t.IntegerType(),False),
StructField("Product_Name",t.StringType(),True),
StructField("Price",t.IntegerType(),True),
StructField("DiscountPercent",IntegerType(),True),    
StructField("update_ts",t.TimestampType(),False)
])

df = spark.createDataFrame(data = sample_data, schema = defSchema)

windowsSpec = Window.orderBy("Product_id")

dfMax = df.withColumn('maxPrice',max('Price').over(windowsSpec)) \
	.withColumn('maxDiscountPercent',max('DiscountPercent').over(windowsSpec)) \
.withColumn('update_ts',max('update_ts').over(windowsSpec)) \

dfMax = dfMax.select(col("Product_id"),
             col("Product_Name"),
             col("maxPrice").alias("Price"),
             col("maxDiscountPercent").alias("DiscountPercent"),
                    F.to_timestamp(col("update_ts")).alias("update_ts"))

dfMax = dfMax.drop_duplicates()
             


dfMax.display()


windowsSpec = Window.partitionBy("Product_id").orderBy(col("update_ts"))
dfRank = df.withColumn('rank',F.rank().over(windowsSpec))
dfRank.display()


