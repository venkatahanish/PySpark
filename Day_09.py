# PySpark Day 9

# Problem: Write a solution to swap the seat ID of every two consecutive students. If the number of students is odd, the id of the last student is not swapped.

# Input: 
_data = [(1,'Abbot'),(2,'Doris'),(3,'Emerson'),(4,'Green'),(5,'Jeames')]
_schema = ['id', 'student']

df = spark.createDataFrame(data = _data, schema=_schema)
df.show()

# Solution:
from pyspark.sql.functions import col , lead , lag , when , coalesce
from pyspark.sql import Window

df_lead = df.withColumn("Next_value" , lead(col("student")).over(Window.orderBy(col("id"))))
df_lead_lag = df_lead.withColumn("Prev_value" , lag(col("student")).over(Window.orderBy(col("id"))))
df_answer = df_lead_lag.withColumn("Exchange seat" , when((col("id") % 2) != 0 , coalesce(col("Next_value") , col("student"))).otherwise(col("Prev_value")))
df_answer.select("id" , "student" , "Exchange seat").show()


#+---+-------+-------------+
#| id|student|Exchange seat|
#+---+-------+-------------+
#| 1 | Abbot | Doris       |
#| 2 | Doris | Abbot       |
#| 3 |Emerson| Green       |
#| 4 | Green | Emerson     |
#| 5 | Jeames| Jeames      |
#+---+-------+-------------+

  
