
import os
os.chdir("C:\workspace\spark\spark-3.0.3-bin-hadoop2.7\python")
os.curdir

from pyspark import SparkContext
sc = SparkContext("local", "First App")

from pyspark.sql import SQLContext

SQLContext = SQLContext(sc)

df = SQLContext.read.json("C:\workspace\project\pysparkproject\module1\cust.json");
df.show()
df.printSchema()

df.filter(df["age"] > 32).show()
df.groupBy(df["dept"]).count().show()

df.groupBy(df["dept"]).\
    agg({"Salary": "avg" , "age":"max" }).show()