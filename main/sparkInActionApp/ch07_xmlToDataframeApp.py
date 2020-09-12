"""
   XML ingestion to a dataframe.

   Source of file: NASA patents dataset -
   https://data.nasa.gov/Raw-Data/NASA-Patents/gquh-watm

   @author rambabu.posa


We use the following code to create spark session:

spark = SparkSession.builder.appName("XML to Dataframe") \
    .master("local[*]") \
    .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.9.0") \
    .getOrCreate()

Here we use "spark.jars.packages" to load additional packages.

Unfortuntely, this code snippet does not work as expected

    .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.9.0")

So we can use "--packages" command-line option to run this application

First, clone the project and cd to net.jgp.books.spark.ch07/src/main/python/lab600_xml_ingestion

spark-submit --packages com.databricks:spark-xml_2.12:0.9.0 xmlToDataframeApp.py

We can also use "--conf" command-line option to run this application as shown below:

spark-submit --conf "spark.jars.packages=com.databricks:spark-xml_2.12:0.9.0"  xmlToDataframeApp.py

"""

from pyspark.sql import SparkSession
import os

current_dir = os.path.dirname(__file__)
relative_path = "../../resources/data/sparkInActionData/nasa-patents.xml"
absolute_file_path = os.path.join(current_dir, relative_path)

# Creates a session on a local master
spark = SparkSession.builder.appName("XML to Dataframe") \
    .master("local[*]") \
    .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.9.0") \
    .getOrCreate()

# Reads a CSV file with header, called books.csv, stores it in a
# dataframe
df = spark.read.format("xml") \
        .option("rowTag", "row") \
        .load(absolute_file_path)

# Shows at most 5 rows from the dataframe
df.show(5)
df.printSchema()

spark.stop()