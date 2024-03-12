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

# 1) My hashtag#SQL Approach: 
#===============

with cte as ( -- Joined the datasets and filtered the second half of the 2021 
select t1.name , t2.score
from instacart_stores as t1 
inner join instacart_reviews as t2 
on t1.id = t2.store_id 
where date_part('month', t1.opening_date) > 6 and date_part('year' , t1.opening_date) = 2021) , 
cte1 as ( -- finding the review types 
select *, case when score > 5 then 'Positive' else 'Negative' end as review_type 
from cte ), 
cte2 as ( -- finding review count for each group
select name , 
sum(case when review_type = 'Positive' then 1 else 0 end) as positive_count , 
sum(case when review_type = 'Negative' then 1 else 0 end) as negative_count , 
count(1) as group_count
from cte1
group by name), cte3 as ( -- finding the ratio of positive reviews with respect to negative review
select name , round((100.0 * positive_count / group_count),2) as positive_review_ratio , 
round((100.0 * negative_count / group_count ),2) as negative_review_ratio
from cte2 
)  
#-- filtering the data where negative_review_ratio is more than 20%
select *
from cte3 
where negative_review_ratio > 20

#2) My hashtag#PySpark Approach

import pyspark







