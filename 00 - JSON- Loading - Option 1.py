from pyspark.sql.types import StructType, StructField, IntegerType, StringType,LongType,ArrayType,DoubleType,DateType,IntegerType,ArrayType
from pyspark.sql.functions import explode,col

df = spark.read.option("multiline",True).json("/FileStore/tables/orderDetails.json")

df = df.withColumn("orders",explode(col("datasets"))).select("filename", \
                                                             "orders.orderId", \
                                                             "orders.customerId", \
                                                             "orders.orderDate", \
                                                             "orders.shipmentDetails", \
                                                             "orders.orderDetails")

df = df.withColumn("shipmentDetails",col("shipmentDetails")).select( "filename", \
                                                                     "orderId", \
                                                                     "customerId", \
                                                                     "orderDate", \
                                                                     "shipmentDetails.*", \
                                                                     "orderDetails")

df = df.withColumn("orderDetails",explode(col("orderDetails"))).select("filename", \
                                                                       "orderId", \
                                                                       "customerId", \
                                                                       "orderDate", \
                                                                       "street", \
                                                                       "city", \
                                                                       "state", \
                                                                       "postalCode", \
                                                                       "country", \
                                                                       "orderDetails.*")

df = df.withColumn("orderDetails",col("totalPrice")).select("filename", \
                                                            "orderId", \
                                                            "customerId", \
                                                            "orderDate", \
                                                            "street", \
                                                            "city", \
                                                            "state", \
                                                            "postalCode", \
                                                            "country", \
                                                            "productId", \
                                                            "quantity", \
                                                            "sequence", \
                                                            "totalPrice.*" )

display(df)
