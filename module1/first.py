from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Home").master("local").getOrCreate()

spark.read.format("csv").load("employee.csv").show(2,False);



