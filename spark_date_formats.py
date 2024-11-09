# Import your libraries
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Start writing code
orders_analysis = orders_analysis.withColumn(
    "week", orders_analysis.week.cast(T.DateType())
)

orders_analysis = orders_analysis.withColumn("quarter", F.quarter("week"))
orders_analysis = orders_analysis.withColumn("year", F.year("week"))

first_quarter_df = orders_analysis.filter(
    (F.col("quarter") == 1) & (F.col("year") == 2023)
)

first_quarter_df = first_quarter_df.withColumn(
    "quantity", F.col("quantity").cast(T.IntegerType())
)
filtered_df = (
    first_quarter_df.select("week", "quantity").groupby("week").agg(F.sum("quantity"))
)

# To validate your solution, convert your final pySpark df to a pandas df
filtered_df.toPandas()
