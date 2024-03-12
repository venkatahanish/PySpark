# pyspark Day 1
# Solving the same
# Data Problem using multiple tech stacks gives a lot of joy.

# Let's try solving this amazing "Negative Reviews in New Locations" interview hashtag#challenges from StrataScratch in hashtag#SQL, 
# hashtag#PySpark which was asked in the Instacart coding interview.

# Problem: Find stores that were opened in the second half of 2021 with more than 20% of their reviews being negative. 
# A review is considered negative when the score given by a customer is below 5. 
# Output the names of the stores together with the ratio of negative reviews to positive ones.

# Schema : 

# Table 1: instacart_reviews

id: int
customer_id : int
store_id : int
score: int
#------------------------------------------------------------------------------------

# Table 2 : instacart_stores

id: int
name: varchar
zipcode: int
opening_date : DateTime


# Pyspark approach

import pyspark
from pyspark.sql.functions import col, month, year, when, sum, count, round
df = instacart_reviews.join(instacart_stores, on = (instacart_reviews.store_id == instacart_stores.id), how = 'inner')\
     .filter((year(col('opening_date')) == 2021) & (month(col('opening_date')) > 6))\
     .withColumn("review_type" , when(col('score') > 5 , "Positive_Review").otherwise("Negative_Review"))\
     .groupby(col('name'))\
     .agg(
       sum(when(col('review_type') == 'Positive_Review', 1).otherwise(0)).alias("Positive_Review_Count"),
       sum(when(col('review_type') == 'Negative_Review', 1).otherwise(0)).alias("Negative_Review_Count"),
       count(col('score')).alias('total_reviews')
     )\
     .withColumn('Positive_Review_Percentage', round((col('Positive_Review_Count')*100.0 / col('total_reviews')), 2))\
     .withColumn('Negative_Review_Percentage', round((col('Negative_Review_Count')* 100 / col('total_reviews')), 2))\
     .filter(col('Negative_Review_Percentage') > 20)\
     .select(col('name'), col('Positive_Review_Percentage'), col('Negative_Review_Percentage'))

df.show()








