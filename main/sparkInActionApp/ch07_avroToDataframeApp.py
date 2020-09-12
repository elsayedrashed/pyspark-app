"""
 Avro ingestion in a dataframe.

 Source of file: Apache Avro project -
 https://github.com/apache/orc/tree/master/examples

 @author rambabu.posa

Please use the following steps to run this application

1. clone the project and cd to the folder

cd net.jgp.books.spark.ch07/src/main/python/lab910_avro_ingestion/

2. Run application

spark-submit --packages org.apache.spark:spark-avro_2.12:3.0.0-preview2 avroToDataframeApp.py

"""
import os
from pyspark.sql import SparkSession

def get_absolute_file_path(path, filename):
    current_dir = os.path.dirname(__file__)
    relative_path = "{}{}".format(path, filename)
    absolute_file_path = os.path.join(current_dir, relative_path)
    return absolute_file_path

def main(spark):
    path = "../../resources/data/sparkInActionData"
    filename = "weather.avro"
    absolute_file_path = get_absolute_file_path(path, filename)
    # Reads an Avro file, stores it in a dataframe
    df = spark.read.format("avro") \
              .load(absolute_file_path)

    # Shows at most 10 rows from the dataframe
    df.show(10)
    df.printSchema()
    print("The dataframe has {} rows.".format(df.count()))


if __name__ == "__main__":

    # Creates a session on a local master
    spark = SparkSession.builder.appName("Avro to Dataframe") \
        .master("local[*]").getOrCreate()
    main(spark)
    spark.stop()