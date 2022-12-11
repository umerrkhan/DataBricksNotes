from pyspark.sql.types import StructType, StructField, IntegerType, StringType,LongType,ArrayType,DoubleType,DateType,IntegerType,ArrayType
df = spark.read.option("multiline",True).json("/FileStore/tables/orderDetails.json")

df = df.withColumn("orders",explode(col("datasets"))).select("filename", \
                                                             "orders.orderId", \
                                                             "orders.customerId", \
                                                             "orders.orderDate", \
                                                             "orders.shipmentDetails.city", \
                                                             "orders.shipmentDetails.country", \
                                                             "orders.shipmentDetails.postalCode", \
                                                             "orders.shipmentDetails.state", \
                                                             "orders.shipmentDetails.street", \
                                                             "orders.orderDetails"
                                                             ) \
        .withColumn("orderDetails2",explode(col("orderDetails"))).select("*", \
                                                                         "orderDetails2.productId", \
                                                                         "orderDetails2.quantity", \
                                                                         "orderDetails2.sequence", \
                                                                         "orderDetails2.totalPrice", \
                                                                         "orderDetails2.totalPrice.gross",\
                                                                         "orderDetails2.totalPrice.net",\
                                                                         "orderDetails2.totalPrice.tax" \
                                                                        ).drop("orderDetails","orderDetails2","totalPrice")


display(df)
print(df.printSchema())
