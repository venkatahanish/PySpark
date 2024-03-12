#PySpark Day 6

# Question: The task is to merge two data frames using PySpark.

# Input:
data1 = [(1,"Sagar" , "CSE" , "UP" , 80),\
 (2,"Shivani" , "IT" , "MP", 86),\
 (3,"Muni", "Mech" , "AP", 70)]
data1_schema = ("ID" , "Student_Name" ,"Department_Name" , "City" , "Marks")
df1 = spark.createDataFrame(data= data1 , schema = data1_schema)
df1.show()

data2 = [(4, "Raj" , "CSE" , "HP") , \
 (5 , "Kunal" , "Mech" , "Rajasthan") ]
data2_scehma = ("ID" , "Student_Name" , "Department_name" , "City")
df2 = spark.createDataFrame(data = data2 , schema = data2_scehma)
df2.show()

#+---+------------+---------------+----+-----+
#| ID|Student_Name|Department_Name|City|Marks|
#+---+------------+---------------+----+-----+
#| 1 | Sagar      | CSE           | UP | 80  |
#| 2 | Shivani    | IT            | MP | 86  |
#| 3 | Muni       | Mech          | AP | 70  |
#+---+------------+---------------+----+-----+

#+---+------------+---------------+---------+
#| ID|Student_Name|Department_name| City    |
#+---+------------+---------------+---------+
#| 4 | Raj        | CSE           | HP      |
#| 5 | Kunal      | Mech          |Rajasthan|
#+---+------------+---------------+---------+



# Solution:
from pyspark.sql.functions import lit
df2 = df2.withColumn("Marks" , lit(None))
final_df = df1.union(df2)
final_df.show()

#+---+------------+---------------+---------+-----+
#| ID|Student_Name|Department_Name| City|Marks    |
#+---+------------+---------------+---------+-----+
#| 1 | Sagar      | CSE           | UP      | 80  |
#| 2 | Shivani    | IT            | MP      | 86  |
#| 3 | Muni       | Mech          | AP      | 70  |
#| 4 | Raj        | CSE           | HP      | Null|
#| 5 | Kunal      | Mech          |Rajasthan| Null|
#+---+------------+---------------+---------+-----+

