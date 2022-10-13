from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
sc = spark.sparkContext
data="D:\\spark\\datasets\\donations.csv"
rdd=sc.textFile(data)
# don=rdd.filter(lambda x:"name" not in x).map(lambda x:x.split(","))

# don=rdd.filter(lambda x:"name" not in x).map(lambda x:x.split(",")).map(lambda x:(x[0],x[2])) >>convert 3rd to int values

# don=rdd.filter(lambda x:"name" not in x).map(lambda x:x.split(",")).map(lambda x:(x[0],int(x[2]))).reduceByKey(lambda x,y:x+y)
'''('venu', 32000)
('anu', 9000)
('venkat', 14000)
('sita', 11000)'''

'''order by donation second col'''
# don=rdd.filter(lambda x:"name" not in x).map(lambda x:x.split(",")).map(lambda x:(x[0],int(x[2])))\
#                .reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[1],ascending=False)
'''op >> ('venu', 32000)
('venkat', 14000)
('sita', 11000)
('anu', 9000)'''
'''order by name second col'''
don=rdd.filter(lambda x:"name" not in x).map(lambda x:x.split(",")).map(lambda x:(x[0],int(x[2])))\
               .reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[0])
for i in don.collect():
    print(i)
