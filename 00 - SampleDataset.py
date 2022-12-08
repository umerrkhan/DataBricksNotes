from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType,TimestampType
from pyspark.sql.functions import col, to_date

dataEmp = [  
    (7698, "BLAKE", "MANAGER", 7839,'01-05-1981', 2850, None, 30 ),
    (7782, 'CLARK', 'MANAGER', 7839,  "09-06-1981", 2450, None, 10),
    (7566, 'JONES', 'MANAGER', 7839,"02-04-1981", 2975, None, 20),
    (7788, 'SCOTT', 'ANALYST', 7566, "13-07-1987",3000, None, 20),
    (7902, 'FORD', 'ANALYST', 7566,"03-12-1981" ,3000, None, 20),
    (7369, 'SMITH', 'CLERK', 7902,"17-12-1980", 800, None, 20 ) ,
    (7499, 'ALLEN', 'SALESMAN', 7698,"20-02-1981" ,1600, "300", 30),
    (7521, 'WARD', 'SALESMAN', 7698, "22-02-1981",1250, "500", 30  ),
    (7654, 'MARTIN', 'SALESMAN', 7698, "28-09-1981", 1250, "1400", 30 ),
    (7844, 'TURNER', 'SALESMAN', 7698, "08-09-1981", 1500, "0", 30 ),
    (7876, 'ADAMS', 'CLERK', 7788,"13-07-1987", 1100, None, 20),
    (7900, 'JAMES', 'CLERK', 7698,"03-12-1981", 950, None, 30),
    (7934, 'MILLER', 'CLERK', 7782,"23-01-1982" ,1300, None, 10 ),
    (7839, 'KING', 'PRESIDENT', None, "17-11-1981", 5000, None, 10) 
          ]
schemaEmp = ["empno","ename","job","mger","hiredate","sal","comm","deptno"]
schemaEmp = StructType(
                    [
                        StructField("empno",IntegerType(),True),
                        StructField("ename",StringType(),True),
                        StructField("job",StringType(),True),
                        StructField("mgr",StringType(),True),
                        StructField("hiredate",StringType(),True),
                        StructField("sal",IntegerType(),True),
                        StructField("comm",StringType(),True),
                        StructField("deptno",IntegerType(),True)
                    ]
)

dataDept = [ (10, 'ACCOUNTING', 'NEW YORK'),
            (20, 'RESEARCH', 'DALLAS'),
            (30, 'SALES', 'CHICAGO'),
             (40, 'OPERATIONS', 'BOSTON')
          ]

schemaDept = StructType([
                StructField("deptno",IntegerType(),True),
                StructField("dname",StringType(),True),
                StructField("loc",StringType(),True)
    
])
    
dfDept = spark.createDataFrame(data=dataDept,schema=schemaDept)
display(dfDept)
    
    
empDf = spark.createDataFrame(data=dataEmp,schema=schemaEmp)
empDf = empDf.withColumn('hiredate',to_date(col('hiredate'), 'dd-MM-yyyy'))
display(empDf)





