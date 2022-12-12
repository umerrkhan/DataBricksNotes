from pyspark.sql.types import StructType, StructField, IntegerType, StringType,LongType,ArrayType,DoubleType,DateType,IntegerType,ArrayType
import  pyspark.sql.functions as F
orderDetailssSchema = StructType([ \
                                  StructField("filename", StringType(), True) \
                                  ,StructField("datasets", ArrayType(\
                                                                     StructType(
                                                                                 [StructField("orderId", StringType(),True),\
                                                                                 StructField("customerId", StringType(),True),\
                                                                                 StructField("orderDate", StringType(),True),\
                                                                                 StructField("orderDetails",ArrayType(StructType([StructField("productId",StringType(),True),\
                                                                                   StructField("quantity",StringType(),True),\
                                                                                   StructField("sequence",StringType(),True),\
                                                                                   StructField("totalPrice",StructType([StructField("gross",StringType(),True),StructField("net",StringType(),True),StructField("tax",StringType(),True) ]),True) ,
                                                                                   
                                                                                   ])),True),                                                                                 
                                                                                 StructField("shipmentDetails",StructType([\
StructField("street", StringType(),True), \
StructField("state",StringType(),True),\
                                                                                                                           StructField("city",StringType(),True),\
StructField("postalCode",StringType(),True),\
                                                                                                                           StructField("country",StringType(),True)\
                                                                                                                          ]),True)] \
                                                                                 ),True))\
                                  
                                 ])
                                  
                                 

df = spark.read.option("multiline",True).schema(orderDetailssSchema).json("/FileStore/tables/orderDetails.json")

#.select("datasets.orderId","datasets.shipmentDetails.street")
print(df.printSchema())
display(df)
