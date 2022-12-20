# ADLS connectivity 

dbutils.fs.mount(
source = "wasbs://container-source@storageaccountwtc.blob.core.windows.net/",
    mount_point = "/mnt/adls/",    
    extra_configs = {"fs.azure.account.key.storageaccountwtc.blob.core.windows.net":"ENMVA6hLfXRhiYoKOnb4jyVjvpjpelASpG82zHxOmWTX6vdW0/hQluFELX8sahj+C+mOCCee94jF+AStAZbcZQ=="}
)
# SQL Server connectivity
jdbcHostname = "sqlserver-ufde.database.windows.net"
jdbcPort = 1433
jdbcDatabase = "MyDevDB"
jdbcUsername = "ufde"
jdbcPassword = "Test123@"
jdbcDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};databaseName={jdbcDatabase};user={jdbcUsername};password={jdbcPassword}"

dfProduct = spark.read.format("jdbc").option("url",jdbcUrl).option("dbtable","SalesLT.Product").load()
dfSalesDetail = spark.read.format("jdbc").option("url",jdbcUrl).option("dbtable","SalesLT.SalesOrderDetail").load()
dfProduct = dfProduct.na.fill({'Size':'M','Weight':'100'}).drop_duplicates()
df = dfProduct.join(dfSalesDetail, (dfProduct.ProductID == dfSalesDetail.ProductID) & (dfProduct.ProductID == dfSalesDetail.ProductID),"leftouter").select(
dfProduct.ProductID,
    dfProduct.Name,
    dfProduct.Color,
    dfProduct.Size,
    dfProduct.Weight,
    dfSalesDetail.UnitPrice,
    dfSalesDetail.LineTotal
)
df = df.groupby("ProductID", "Name", "Color", "Size", "Weight").sum("LineTotal").withColumnRenamed("sum(LineTotal)","TotalSales")
df = df.groupby("ProductID").sum("TotalSales")
df.write.format("parquet").save("/mnt/adls/parquet_results/")
df.write.format("csv").option("header","true").save("/mnt/adls/csv_results/results.csv")

display(df)
