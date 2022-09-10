'''REAL TIME USE CASE OF RDD FOR UNSTUCTURED DATA '''
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
sc=spark.sparkContext
data="D:\spark\datasets\emailsmay4.txt"
erdd=sc.textFile(data)
# for i in erdd.collect():
#     print(i)
'''TO get email_id from data'''
# res=erdd.flatMap(lambda x:x.split(" ")).filter(lambda x:"@" in x)
'''TO get email_id as well as username from data'''
res=erdd.filter(lambda x:"@" in x).map(lambda x:x.split(" ")).map(lambda x:(x[0],x[-1]))
for i in res.collect():
    print(i)
