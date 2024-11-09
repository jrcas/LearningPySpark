# Import your libraries
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# Start writing code
# sales_data = sales_data.filter(F.col("month").startswith("2024-01"))
sales_data = (
    sales_data.withColumn("month", F.col("month").cast(T.DateType()))
    .withColumn("month_num", F.month("month"))
    .where("month_num == 1")
    .drop("month_num")
)

# sales_data = sales_data.filter(col('month').startswith('2024-01'))

window_spec = Window.partitionBy("product_category").orderBy(F.desc("total_sales"))

sales_data = (
    sales_data.withColumn("rank", F.row_number().over(window_spec))
    .filter(F.col("rank") <= 3)
    .drop("rank")
)

# To validate your solution, convert your final pySpark df to a pandas df
sales_data.toPandas()
