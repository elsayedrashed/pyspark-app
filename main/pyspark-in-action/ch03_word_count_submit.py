from pyspark.sql import SparkSession
import pyspark.sql.functions as F


spark = SparkSession.builder.appName(
    "Counting word occurences in a document."
).getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# If you need to read multiple text files, replace `1342-0` by `*`.
results = (
    spark.read.text("../../resources/data/shakespeare.txt")
    .select(F.split(F.col("value"), " ").alias("line"))
    .select(F.explode(F.col("line")).alias("word"))
    .select(F.lower(F.col("word")).alias("word"))
    .select(F.regexp_extract(F.col("word"), "[a-z']*", 0).alias("word"))
    .where(F.col("word") != "")
    .groupby(F.col("word"))
    .count()
)

results.orderBy("count", ascending=False).show(10)

# Save result
# results.coalesce(1).write.csv("./results_single_partition.csv")
