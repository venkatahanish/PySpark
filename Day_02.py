## spark Day 2

#Let's try solving this amazing "Player with longest streak" hashtag#interview coding question using hashtag#PySpark, 
#Pandas and #SQL which was asked in Google and Amazon.

#Task: You are given a table of tennis players and their matches that they could either win (W) or lose (L). Find the longest streak of wins. A streak is a set of consecutive matches of one player. The streak ends once a player loses their next match. Output the ID of the player or players and the length of the streak.

#Schema:
#======
#player_id: int
#match_date: datetime
#match_result: varchar

#PySpark Approach : 
#================
import pyspark
from pyspark.sql import Window
from pyspark.sql.functions import row_number, col, count , dense_rank , desc


df = players_results.withColumn("rn" , row_number().over(Window.partitionBy(col("player_id")).orderBy("match_date")))

df = df.filter(col("match_result") == 'W') 

df = df.withColumn("rn_diff" , col("rn") - row_number().over(Window.partitionBy(col("player_id")).orderBy("match_date")))

df = df.groupBy(col("player_id") , col("rn_diff"))\
 .agg(count(col("match_date")).alias("winning_streak_count"))

df = df.withColumn("winners_rank" , dense_rank().over(Window.orderBy(desc("winning_streak_count"))))\
 .filter(col("winners_rank") == 1 )\
 .select(col("player_id"), col("winning_streak_count"))

df.show()

