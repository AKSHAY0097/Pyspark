from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
sc = spark.sparkContext
data = "D:\\spark\\bigdata\\datasets\\asl.csv"
erdd=sc.textFile(data)
'''select * from asl'''
res=erdd.map(lambda x:x.split(",")).filter(lambda x:"name" not in x)

'''select count(*) from asl group by city'''
# res=erdd.map(lambda x:x.split(",")).filter(lambda x:"name" not in x).map(lambda x:(x[2],1)) >>>genrate key value pair
# res=erdd.map(lambda x:x.split(",")).filter(lambda x:"name" not in x).map(lambda x:(x[2],1)).reduceByKey(lambda x,y:x+y)
for i in res.collect():
    print(i)
