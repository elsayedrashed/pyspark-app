"""
  Simple SQL select on ingested data

To test:
 * Run simpleSelectGlobalViewApp first
 * You have 60s to run simpleSelectGlobalViewFailedReuseApp, but you will see that the view is not shared between the dataFrame...

"""

from pyspark.sql import SparkSession

# Creates a session on a local master
spark = SparkSession.builder.appName("Simple SELECT using SQL") \
    .master("local[*]") \
    .getOrCreate()

query = """
  SELECT * FROM global_temp.geodata
  WHERE yr1980 > 1
  ORDER BY 2
  LIMIT 5
"""

# This will fail as it is not the same dataFrame
smallCountries = spark.sql(query)

# Shows at most 10 rows from the dataFrame (which is limited to 5 anyway)
smallCountries.show(10, False)

# Good to stop SparkSession at the end of the dataFrame
spark.stop()
