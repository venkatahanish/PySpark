#PySpark Day 5

# Question: Calculate the % Marks for each student. Each subject is worth 100 marks. Create a result column by following the below condition 

 # 1. % Marks greater than or equal to 70 then 'Distinction'
 # 2. % Marks range between 60-69 then 'First Class'
 # 3. % Marks range between 50-59 then 'Second Class'
 # 4. % Marks range between 40-49 then 'Third Class'
 # 5. % Marks Less than or equal to 39 then 'Fail'

# Input:
student = [(1,'Steve'),(2,'David'),(3,'Aryan')]
student_schema = "student_id int , student_name string"

marks = [(1,'pyspark',90),
 (1,'sql',100),
 (2,'sql',70),
 (2,'pyspark',60),
 (3,'sql',30),
 (3,'pyspark',20)]
marks_schema = "student_id int , subject_name string , marks int"

# Create Student dataFrame 
student_df = spark.createDataFrame(data = student , schema = student_schema)
student_df.show()

# Create Marks DataFrame 
marks_df = spark.createDataFrame(data = marks , schema = marks_schema)
marks_df.show()

#+----------+------------+
#|student_id|student_name|
#+----------+------------+
#| 1        | Steve      |
#| 2        | David      |
#| 3        | Aryan      |
#+----------+------------+

#+----------+------------+-----+
#|student_id|subject_name|marks|
#+----------+------------+-----+
#| 1        | pyspark    | 90  |
#| 1        | sql        | 100 |
#| 2        | sql        |  70 |
#| 2        | pyspark    | 60  |
#| 3        | sql        | 30  |
#| 3        | pyspark    | 20  |
#+----------+------------+-----+


# Solution:
from pyspark.sql.functions import col , sum , count ,when
df1=marks_df.groupBy(col("student_id")).agg(sum(col("marks")).alias("total_marks") , count(col("subject_name")).alias("total_subject"))
df2 = df1.withColumn("Percentage" , col("total_marks")/col("total_subject")) 
df3 = df2.withColumn("Result" , when(col("Percentage") > 70 , "Distinction")\
 .when(col("Percentage").between(60,69) , "First Class")\
 .when(col("Percentage").between(50,59) , "Second Class")\
 .when(col("Percentage").between(40,49) , "Third Class")\
 .otherwise("Fail")
 ) 
df3.show()

#+----------+-----------+-------------+----------+-----------+
#|student_id|total_marks|total_subject|Percentage| Result    |
#+----------+-----------+-------------+----------+-----------+
#| 1        |        190| 2           | 95.0     |Distinction|
#| 2        |        130| 2           | 65.0     |First Class|
#| 3        |         50| 2           | 25.0     | Fail      |
#+----------+-----------+-------------+----------+-----------+


