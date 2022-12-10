# Without Specifying schema
df = spark.read.format("csv").option("inferschema",True).option("header",True).option("sep",",").load("/FileStore/tables/dept.csv")

# With Specifying schema
from pyspark.sql.types import *
deptSchema = StructType(
[
    StructField('deptno',IntegerType()),
    StructField('dname',StringType()),
    StructField('loc',StringType())
])

df = spark.read.format("csv").schema(deptSchema).option("sep",",").option("header",True).load("/FileStore/tables/dept.csv")

display(df)

print(df.count())

display(df.printSchema())
