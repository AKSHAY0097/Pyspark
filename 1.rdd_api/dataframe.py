from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
data = "D:\\spark\\datasets\\asldata.txt"
sc=spark.sparkContext
sc.setLogLevel("ERROR")

aslrdd=sc.textFile(data)
res=aslrdd.map(lambda x: x.split(",")).toDF(["name","age","city"])

'''TO SKIP THE HEADER '''
# res=aslrdd.filter(lambda x:"name" not in x).map(lambda x: x.split(",")).toDF(["name","age","city"])

res.show()

'''USE OF SPARK SQL'''
res.createTempView("asl")
# result=spark.sql("select * from asl")
# result=spark.sql("select * from asl where city='blr' and age < 30")
# result.show()

'''by programming'''
result=res.where((col('city')=='blr') & ((col('age')<30)))
# result.show()


