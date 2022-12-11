 empSchema = StructType(
[
    StructField('empno',IntegerType()),
    StructField('ename',StringType()),
    StructField('job',StringType()),
    StructField('mgr',StringType()),
    StructField('hiredate',DateType()),
    StructField('sal',IntegerType()),
    StructField('comm',IntegerType()),
    StructField('deptno',StringType()),
    StructField('_corrupt_record',StringType())
])
## Mode PERMISSIVE
df = spark.read \
.format("csv") \
.schema(empSchema) \
.option("mode","PERMISSIVE") \
.option("sep",",") \
.option("header",True) \
.option("nullValue", "null") \
.option("dateFormat", "dd/MM/yyyy") \
.load("/FileStore/tables/emp.csv")
display(df)


 empSchema = StructType(
[
    StructField('empno',IntegerType()),
    StructField('ename',StringType()),
    StructField('job',StringType()),
    StructField('mgr',StringType()),
    StructField('hiredate',StringType()), ### Changed the type to string 
    StructField('sal',IntegerType()),
    StructField('comm',IntegerType()),
    StructField('deptno',StringType()),
    StructField('_corrupt_record',StringType())
])

 ## Mode DROPMALFORMED
 # Bad Records will be dropped. File contain 19 records and 4 of them dropped due to invalid Format. 
 # To see the reasoning of droping a row use PERMISSIVE.
dfemp = spark.read.format("csv").schema(empSchema).option("mode","DROPMALFORMED").option("sep",",").option("header",True).load("/FileStore/tables/emp.csv")
display(dfemp)

## Mode FAILFAST
# if there would be any Bad Records in the whole file it will reject the whole statement and give you reasoning why this happend
dfemp = spark.read.format("csv").schema(empSchema).option("mode","FAILFAST").option("sep",",").option("header",True).load("/FileStore/tables/emp.csv")
display(dfemp)

 




