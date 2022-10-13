from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
sc = spark.sparkContext
data="D:\\spark\\datasets\\donations.csv"
rdd=sc.textFile(data)
# for i in rdd.collect():
#      print(i)

don=rdd.filter(lambda x:"name" not in x).map(lambda x:x.split(",")).map(lambda x:x[0]).distinct()
# for i in don.collect():
     # print(i)

