display(
    empDf.filter(  
    (col('mgr').isNotNull()) & 
    ((col('deptno') == 10) | (col('deptno') == 30)) &
    (col('job').isin('SALESMAN','CLERK','MANAGER')) &
    (col('sal') > 100) &
    ((to_date(col('hiredate'), 'dd-MM-yyyy') > "1981-01-31" ) &     (to_date(col('hiredate'), 'dd-MM-yyyy') < "1990-04-30"))     
                 )        
       )
