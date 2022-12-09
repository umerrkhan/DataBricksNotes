display(empDf.groupBy("job").pivot("deptno").min("sal"))
