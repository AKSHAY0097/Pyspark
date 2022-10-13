from pyspark.sql import *
from pyspark.sql.functions import *
spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
sc=spark.sparkContext

data=[2,3,4,5,6]
drdd=sc.parallelize(data)
for i in drdd.collect():
    print(i)



