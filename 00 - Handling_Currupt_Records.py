 empSchema = StructType(
[
    StructField('empno',IntegerType()),
    StructField('ename',StringType()),
    StructField('job',StringType()),
    StructField('mgr',StringType()),
    StructField('hiredate',StringType()),
    StructField('sal',IntegerType()),
    StructField('comm',IntegerType()),
    StructField('deptno',StringType()),
    StructField('_corrupt_record',StringType())
])
df = spark.read.format("csv").schema(empSchema).option("mode","PERMISSIVE").option("sep",",").option("header",True).load("/FileStore/tables/emp.csv")
