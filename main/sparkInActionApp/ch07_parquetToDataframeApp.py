"""
 Parquet ingestion in a dataframe.

 Source of file: Apache Parquet project -
 https://github.com/apache/parquet-testing

 @author rambabu.posa
"""
from pyspark.sql import SparkSession
import os

# Creates a session
def create_spark_session(appname, master):
    return SparkSession.builder.appName(appname).master(master).getOrCreate()

# Get absolute file path
def get_absolute_file_path(path, filename):
    current_dir = os.path.dirname(__file__)
    relative_path = "{}/{}".format(path, filename)
    return os.path.join(current_dir, relative_path)

# Main function
def main():
    path = "../../resources/data/sparkInActionData"
    filename = "alltypes_plain.parquet"
    absolute_file_path = get_absolute_file_path(path, filename)
    # Creat Spark Session
    spark = create_spark_session("Parquet to Dataframe", "local[*]")
    # Reads an parquet file, stores it in a dataframe
    df = spark.read.format("parquet") \
              .load(absolute_file_path)

    # Shows at most 10 rows from the dataframe
    df.show(10)
    df.printSchema()
    print("The dataframe has {} rows.".format(df.count()))
    spark.stop()

if __name__ == "__main__":
    main()
