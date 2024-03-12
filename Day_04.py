#PySpark Day 4

# Question: Check the count of null values in each column in the data frame.

# Input:
employee = [(1, 'Sagar' ,23),(2, None , 34),(None ,'John' , 46),(5,'Alex', None) , (4,'Alice',None)]
employee_schema = "emp_id int , name string , age int"
emp_df = spark.createDataFrame(data = employee , schema = employee_schema)
emp_df.show()

#+------+-----+----+
#|emp_id| name| age|
#+------+-----+----+
#| 1|Sagar| 23|
#| 2| null| 34|
#| null| John| 46|
#| 5| Alex|null|
#| 4|Alice|null|
#+------+-----+----+



#Solution:
from pyspark.sql.functions import col , count ,when
df1 = emp_df.select([count(when(col(i).isNull(),1)).alias(i) for i in emp_df.columns])
df1.show()

#+------+----+---+
#|emp_id|name|age|
#+------+----+---+
#| 1| 1| 2|
#+------+----+---+



