## Reading Data from SQLSERVER

# Option -1 
connectionString = "jdbc:sqlserver://sqlserver-ufde2019.database.windows.net:1433;database=MyDevDB;user=ufde@sqlserver-ufde;password=ERROR23@;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
df = spark.read.jdbc(connectionString,"SalesLT.Product")
display(df)



# Option -2
jdbcHostname = "sqlserver-ufde2019.database.windows.net"
jdbcPort = 1433
jdbcDatabase = "MyDevDB"
jdbcUsername = "ufde"
jdbcPassword = "Error123@"
jdbcDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};databaseName={jdbcDatabase};user={jdbcUsername};password={jdbcPassword}"

df = spark.read.format("jdbc").option("url",jdbcUrl).option("dbtable","SalesLT.Product").load()
print(df.printSchema())

