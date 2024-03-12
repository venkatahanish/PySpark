# PySpark Day 7

# Question: Find the origin and the destination of each customer.

#Input:
flights_data = [(1,'Flight1' , 'Delhi' , 'Hyderabad'),
 (1,'Flight2' , 'Hyderabad' , 'Kochi'),
 (1,'Flight3' , 'Kochi' , 'Mangalore'),
 (2,'Flight1' , 'Mumbai' , 'Ayodhya'),
 (2,'Flight2' , 'Ayodhya' , 'Gorakhpur')
 ]

_schema = "cust_id int, flight_id string , origin string , destination string"

df_flight = spark.createDataFrame(data = flights_data , schema= _schema)
df_flight.show()

#+-------+---------+---------+-----------+
#|cust_id|flight_id| origin  |destination|
#+-------+---------+---------+-----------+
#| 1     | Flight1 | Delhi   | Hyderabad |
#| 1     | Flight2 |Hyderabad| Kochi     |
#| 1     | Flight3 | Kochi   | Mangalore |
#| 2     | Flight1 | Mumbai  | Ayodhya   |
#| 2     | Flight2 | Ayodhya | Gorakhpur |
#+-------+---------+---------+-----------+



# Solution:
from pyspark.sql import Window
from pyspark.sql.functions import row_number , when , col , min , max

df1 = df_flight.withColumn("rn" , row_number().over(Window.partitionBy(col("cust_id")).orderBy(col("flight_id"))))

df2 = df1.groupBy(col("cust_id")).agg(
 min(col("rn")).alias("start"),
 max(col("rn")).alias("end"))

df3 = df1.join(df2 , on = (df1.cust_id == df2.cust_id)).drop(df2.cust_id)
df_final = df3.groupBy(col("cust_id")).agg(
 max(when(col("rn") == col("start") , col("Origin"))).alias("Origin"),
 max(when(col("rn") == col("end") , col("Destination"))).alias("Destination"))

df_final.show()


#+-------+------+-----------+
#|cust_id|Origin|Destination|
#+-------+------+-----------+
#| 1     | Delhi| Mangalore |
#| 2     |Mumbai| Gorakhpur |
#+-------+------+-----------+

