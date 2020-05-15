from pyspark import SparkContext, SparkConf
# from pyspark.sql import SparkSession

conf = SparkConf().setAppName('MyFirstStandaloneApp')
sc = SparkContext(conf=conf)

# do something to prove it works
# spark.sql('SELECT "Test" as c1').show()

# spark = SparkSession.builder.appName("SimpleApp").getOrCreate()

text_file = sc.textFile("../resources/data/shakespeare.txt")
# text_file = sc.textFile("hdfs:///tmp/shakespeare.txt")

counts = text_file.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)

print ("Number of elements: " + str(counts.count()))
counts.saveAsTextFile("./shakespeareWordCount")
# counts.saveAsTextFile("hdfs:///tmp/shakespeareWordCount")