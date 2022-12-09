from pyspark.sql.functions import *
from pyspark.sql.types import *

display(
    empDf.withColumn('newhiredateFormat',date_format(col('hiredate'), 'yyyy-MMM-dd'))
        .withColumn('Prohibation_EndDate',date_add('hiredate', 90))    
         .withColumn('Insert_date',current_timestamp()	)    
         .select('Empno'
                 ,'HireDate'
                 ,next_day( "HireDate", "mon").alias("Actual_WorkStartDate")
                 ,last_day( "HireDate").alias("Salary_Date")                 
                 ,datediff(last_day( "HireDate"),next_day( "HireDate", "mon")).alias("workingsDays")
                 ,'newhiredateFormat'
                 ,'Prohibation_EndDate'
                 ,((year("HireDate")*100)+(month("HireDate")))
                 ,'insert_date'
                )         
)
