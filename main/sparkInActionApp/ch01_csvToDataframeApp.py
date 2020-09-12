"""
CsvToDataframeApp.py - CSV ingestion in a dataFrame.
@author rambabu.posa
"""
from pyspark.sql import SparkSession
import os

current_dir = os.path.dirname(__file__)
relative_path = "../../resources/data/sparkInActionData/books.csv"
absolute_file_path = os.path.join(current_dir, relative_path)

# Creates a session on a local master
session = SparkSession.builder.appName("CSV to Dataset").master("local[*]").getOrCreate()

# Reads a CSV file with header, called books.csv, stores it in a dataFrame
df = session.read.csv(header=True, inferSchema=True, path=absolute_file_path)

# Shows at most 5 rows from the dataFrame
df.show(5)

# Good to stop SparkSession at the end of the dataFrame
session.stop()