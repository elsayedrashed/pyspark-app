from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
import pyspark.sql.functions as F
import pyspark.sql.types as T

spark = SparkSession.builder.getOrCreate()

DATA_DIRECTORY = "../../resources/"

backblaze_2019 = spark.read.csv(
    DATA_DIRECTORY + "drive_stats", header=True, inferSchema=True
)

# Setting the layout for each column according to the schema

q = backblaze_2019.select(
    [
        F.col(x).cast(T.LongType()) if x.startswith("smart") else F.col(x)
        for x in backblaze_2019.columns
    ]
)

backblaze_2019.createOrReplaceTempView("backblaze_stats_2019")

spark.sql(
   "select serial_number from backblaze_stats_2019 where failure = 1"
).show(5)

backblaze_2019.where("failure = 1").select(F.col("serial_number")).show(5)

# +-------------+
# |serial_number|
# +-------------+
# |    57GGPD9NT|
# |     ZJV02GJM|
# |     ZJV03Y00|
# |     ZDEB33GK|
# |     Z302T6CW|
# +-------------+
# only showing top 5 rows

spark.sql(
    """SELECT
           model,
           min(capacity_bytes / pow(1024, 3)) min_GB,
           max(capacity_bytes/ pow(1024, 3)) max_GB
        FROM backblaze_stats_2019
        GROUP BY 1
        ORDER BY 3 DESC"""
).show(5)

backblaze_2019.groupby(F.col("model")).agg(
    F.min(F.col("capacity_bytes") / F.pow(F.lit(1024), 3)).alias("min_GB"),
    F.max(F.col("capacity_bytes") / F.pow(F.lit(1024), 3)).alias("max_GB"),
).orderBy(F.col("max_GB"), ascending=False).show(5)

# --------------------+--------------------+-------+
# |               model|              min_GB| max_GB|
# +--------------------+--------------------+-------+
# | TOSHIBA MG07ACA14TA|             13039.0|13039.0|
# |       ST12000NM0007|-9.31322574615478...|11176.0|
# |HGST HUH721212ALN604|             11176.0|11176.0|
# |       ST10000NM0086|              9314.0| 9314.0|
# |HGST HUH721010ALE600|              9314.0| 9314.0|
# +--------------------+--------------------+-------+
# only showing top 5 rows

spark.sql(
    """SELECT
           model,
           min(capacity_bytes / pow(1024, 3)) min_GB,
           max(capacity_bytes/ pow(1024, 3)) max_GB
        FROM backblaze_stats_2019
        GROUP BY 1
        HAVING min_GB != max_GB
        ORDER BY 3 DESC"""
).show(5)

backblaze_2019.groupby(F.col("model")).agg(
    F.min(F.col("capacity_bytes") / F.pow(F.lit(1024), 3)).alias("min_GB"),
    F.max(F.col("capacity_bytes") / F.pow(F.lit(1024), 3)).alias("max_GB"),
).where(F.col("min_GB") != F.col("max_GB")).orderBy(
    F.col("max_GB"), ascending=False
).show(
    5
)

# +--------------------+--------------------+-------+
# |               model|              min_GB| max_GB|
# +--------------------+--------------------+-------+
# |       ST12000NM0007|-9.31322574615478...|11176.0|
# +--------------------+--------------------+-------+
# only showing top 5 rows

spark.sql(
    """
    CREATE VIEW drive_days AS
        SELECT model, count(*) AS drive_days
        FROM backblaze_stats_2019
        GROUP BY model"""
)

spark.sql(
    """CREATE VIEW failures AS
           SELECT model, count(*) AS failures
           FROM backblaze_stats_2019
           WHERE failure = 1
           GROUP BY model"""
)

drive_days = backblaze_2019.groupby(F.col("model")).agg(
    F.count(F.col("*")).alias("drive_days")
)

failures = (
    backblaze_2019.where(F.col("failure") == 1)
    .groupby(F.col("model"))
    .agg(F.count(F.col("*")).alias("failures"))
)

q.createOrReplaceTempView("Q1")

spark.sql(
    """
    CREATE VIEW backblaze_2019 AS   
    SELECT {col} FROM Q1
""".format(
        col=columns_backblaze
    )
)

backblaze_2019 = (  # <3>
    q.select(q.columns)
)

spark.sql(
    """select
           drive_days.model,
           drive_days,
           failures
    from drive_days
    left join failures
    on
        drive_days.model = failures.model"""
).show(5)

drive_days.join(failures, on="model", how="left").show(5)

spark.sql(
    """
    SELECT
        model,
        failures / drive_days failure_rate
    FROM (
        SELECT
            model,
            count(*) AS drive_days
        FROM drive_stats
        GROUP BY model) drive_days
    INNER JOIN (
        SELECT
            model,
            count(*) AS failures
        FROM drive_stats
        WHERE failure = 1
        GROUP BY model) failures
    ON
        drive_days.model = failures.model
    ORDER BY 2 desc
    """
).show(5)


# tag::ch07-code-cte[]
spark.sql(
    """
    WITH drive_days as (
        SELECT
            model,
            count(*) AS drive_days
        FROM drive_stats
        GROUP BY model),
    failures as (
        SELECT
            model,
            count(*) AS failures
        FROM drive_stats
        WHERE failure = 1
        GROUP BY model)
    SELECT
        model,
        failures / drive_days failure_rate
    FROM drive_days
    INNER JOIN failures
    ON
        drive_days.model = failures.model
    ORDER BY 2 desc
    """
).show(5)
# end::ch07-code-cte[]

# tag::ch07-code-pyspark-cte[]
def failure_rate(drive_stats):
    drive_days = drive_stats.groupby(F.col("model")).agg(  # <1>
        F.count(F.col("*")).alias("drive_days")
    )

    failures = (
        drive_stats.where(F.col("failure") == 1)
        .groupby(F.col("model"))
        .agg(F.count(F.col("*")).alias("failures"))
    )
    answer = (  # <2>
        drive_days.join(failures, on="model", how="inner")
        .withColumn("failure_rate", F.col("failures") / F.col("drive_days"))
        .orderBy(F.col("failure_rate").desc())
    )
    return answer


failure_rate(drive_stats).show(5)

print("drive_days" in dir())  # <3>
# end::ch07-code-pyspark-cte[]
