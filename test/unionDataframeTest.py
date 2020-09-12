"""
   Test Union of two dataframes.

"""
from packages.dataFrame import dataFrameUtility as su
from pyspark.sql import SparkSession
from pyspark.sql.functions import (lit,col,concat,split)
import os

"""
* Builds the dataFrame containing the Wake county restaurants
*
* @return A dataFrame
"""
def build_wake_restaurants_dataframe(df):
    drop_cols = ["OBJECTID", "GEOCODESTATUS", "PERMITID"]
    df = df.withColumn("county", lit("Wake")) \
        .withColumnRenamed("HSISID", "datasetId") \
        .withColumnRenamed("NAME", "name") \
        .withColumnRenamed("ADDRESS1", "address1") \
        .withColumnRenamed("ADDRESS2", "address2") \
        .withColumnRenamed("CITY", "city") \
        .withColumnRenamed("STATE", "state") \
        .withColumnRenamed("POSTALCODE", "zip") \
        .withColumnRenamed("PHONENUMBER", "tel") \
        .withColumnRenamed("RESTAURANTOPENDATE", "dateStart") \
        .withColumn("dateEnd", lit(None)) \
        .withColumnRenamed("FACILITYTYPE", "type") \
        .withColumnRenamed("X", "geoX") \
        .withColumnRenamed("Y", "geoY") \
        .drop("OBJECTID", "GEOCODESTATUS", "PERMITID")

    df = df.withColumn("id",
                       concat(col("state"), lit("_"), col("county"), lit("_"), col("datasetId")))

    df.show(5)
    df.printSchema()
    print("We have {} records in wake_restaurants_dataframe.".format(df.count()))

    # I left the following line if you want to play with repartitioning
    # df = df.repartition(4);
    return df

"""
* Builds the dataFrame containing the Durham county restaurants
*
* @return A dataFrame
"""
def build_durham_restaurants_dataframe(df):
    drop_cols = ["fields", "geometry", "record_timestamp", "recordid"]
    df =  df.withColumn("county", lit("Durham")) \
            .withColumn("datasetId", col("fields.id")) \
            .withColumn("name", col("fields.premise_name")) \
            .withColumn("address1", col("fields.premise_address1")) \
            .withColumn("address2", col("fields.premise_address2")) \
            .withColumn("city", col("fields.premise_city")) \
            .withColumn("state", col("fields.premise_state")) \
            .withColumn("zip", col("fields.premise_zip")) \
            .withColumn("tel", col("fields.premise_phone")) \
            .withColumn("dateStart", col("fields.opening_date")) \
            .withColumn("dateEnd", col("fields.closing_date")) \
            .withColumn("type", split(col("fields.type_description"), " - ").getItem(1)) \
            .withColumn("geoX", col("fields.geolocation").getItem(0)) \
            .withColumn("geoY", col("fields.geolocation").getItem(1)) \
            .drop(*drop_cols)

    df = df.withColumn("id",
                       concat(col("state"), lit("_"), col("county"), lit("_"), col("datasetId")))
    df.show(5)
    df.printSchema()
    print("We have {} records in durham_restaurants_dataframe.".format(df.count()))

    # I left the following line if you want to play with repartitioning
    # df = df.repartition(4);
    return df

current_dir = os.path.dirname(__file__)
relative_path1 = "../resources/data/sparkInActionData/Restaurants_in_Wake_County_NC.csv"
absolute_file_path1 = os.path.join(current_dir, relative_path1)

relative_path2 = "../resources/data/sparkInActionData/Restaurants_in_Durham_County_NC.json"
absolute_file_path2 = os.path.join(current_dir, relative_path2)

# Creates a session on a local master
spark = SparkSession.builder.appName("Union of two dataframes") \
    .master("local[*]").getOrCreate()

df1 = spark.read.csv(path=absolute_file_path1,header=True,inferSchema=True)

df2 = spark.read.json(absolute_file_path2)

wakeRestaurantsDf = build_wake_restaurants_dataframe(df1)
durhamRestaurantsDf = build_durham_restaurants_dataframe(df2)

# Combine dataframes
df = su.combineDataframes(wakeRestaurantsDf, durhamRestaurantsDf)

df.show(5)
df.printSchema()
print("We have {} records in the combined dataframe.".format(df.count()))
partition_count = df.rdd.getNumPartitions()
print("Partition count: {}".format(partition_count))