import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName(
    "Getting the Canadian TV channels with the highest and lowest proportion of commercials."
).getOrCreate()

spark.sparkContext.setLogLevel("WARN")

###############################################################################
# Reading all the relevant data sources
###############################################################################

DIRECTORY = "../../resources/broadcast-logs-data"

logs = spark.read.csv(
    os.path.join(DIRECTORY, "BroadcastLogs.csv"),
    sep="|",
    header=True,
    inferSchema=True,
)

log_identifier = spark.read.csv(
    os.path.join(DIRECTORY, "LogIdentifier.csv"),
    sep="|",
    header=True,
    inferSchema=True,
)

cd_category = spark.read.csv(
    os.path.join(DIRECTORY, "CD_Category.csv"),
    sep="|",
    header=True,
    inferSchema=True,
).select(
    "CategoryID",
    "CategoryCD",
    F.col("EnglishDescription").alias("Category_Description"),
)

cd_program_class = spark.read.csv(
    os.path.join(DIRECTORY, "CD_ProgramClass.csv"),
    sep="|",
    header=True,
    inferSchema=True,
).select(
    "ProgramClassID",
    "ProgramClassCD",
    F.col("EnglishDescription").alias("ProgramClass_Description"),
)

###############################################################################
# Data processing
###############################################################################

logs = logs.drop("BroadcastLogID", "SequenceNO")

logs = logs.withColumn(
    "duration_seconds",
    (
        F.col("Duration").substr(1, 2).cast("int") * 60 * 60
        + F.col("Duration").substr(4, 2).cast("int") * 60
        + F.col("Duration").substr(7, 2).cast("int")
    ),
)

log_identifier = log_identifier.where(F.col("PrimaryFG") == 1)

logs_and_channels = logs.join(log_identifier, "LogServiceID")

full_log = logs_and_channels.join(cd_category, "CategoryID", how="left").join(
    cd_program_class, "ProgramClassID", how="left"
)

full_log.groupby("LogIdentifierID").agg(
    F.sum(
        F.when(
            F.trim(F.col("ProgramClassCD")).isin(
                ["COM", "PRC", "PGI", "PRO", "LOC", "SPO", "MER", "SOL"]
            ),
            F.col("duration_seconds"),
        ).otherwise(0)
    ).alias("duration_commercial"),
    F.sum("duration_seconds").alias("duration_total"),
).withColumn(
    "commercial_ratio", F.col("duration_commercial") / F.col("duration_total")
).orderBy(
    "commercial_ratio", ascending=False
).show(
    1000, False
)
