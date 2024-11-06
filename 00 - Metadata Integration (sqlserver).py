jdbcUrl = "jdbc:sqlserver://witechmill.database.windows.net:1433;database=mymetadatadb;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=600"

token=TokenLibrary.getConnectionString("LinkedService_METADATA_SQLDatabase")
entityName = 'medicalrecordsectionviewmetric'
pushdown_query = f"(SELECT TableName,ColumnName,ColumnDataType FROM [metadata].[Sourceentity] e INNER JOIN [metadata].[entityattribute] a on a.sourceentityid = e.sourceentityid Where entityName = '{entityName}') as tbl"
print(pushdown_query)
connectionProperties = {
 "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver",
 "accessToken" : token
}
df = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)
display(df)
