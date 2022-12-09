from pyspark.sql.functions import col, to_date, expr,when,concat,lit,substring,current_date
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType,TimestampType

display(
    empDf
          .withColumn('YearsSinceEmployed', ((datediff(current_date(),col('hiredate')))/365))
    
          .withColumn('Year',substring('hiredate',1,4))
          .withColumn('Month',substring('hiredate',6,2))
          .withColumn('day',substring('hiredate',9,2))   
    
          .withColumn('AnnualSalary',empDf.sal*12)
          .withColumn('SalaryReview', when( empDf.sal < 1000,
                                                         expr("(AnnualSalary*0.20)/12")
                                         )
                                     .when((empDf.sal > 1000) & (empDf.sal < 2000),
                                                         expr("(AnnualSalary*0.10)/12")
                                          )
                                     .otherwise(
                                                         expr("(AnnualSalary*0.05)/12")
                                          )
                    )
        .withColumn('Comments',concat('ename',lit(' has hired as a ') ,'job', lit(' on '), 'hiredate'))                                          
         
)
