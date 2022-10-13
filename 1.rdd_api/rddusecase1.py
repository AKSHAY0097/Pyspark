from pyspark.sql import *
from pyspark.sql.functions import *
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

data = "D:\\spark\\bigdata\\drivers\\asl.csv"
sc=spark.sparkContext
# sc.setLogLevel("ERROR")
aslrdd=sc.textFile(data)

'''logic to find out people from hydrabad'''
# res = aslrdd.filter(lambda x: "hyd" in x)

'''op >>>>  geting all records wich contains hyd'''

'''convert to array and get record only from city column'''

# res=aslrdd.map(lambda x:x.split(",")) #IN form of array


# res=aslrdd.map(lambda x: x.split(",")).filter(lambda x:"hyd" in x[2])
'''op >>> ['venu', '32', 'hyd']
         ['blru', '11', 'hyd'] '''
#find people having age 11 and from hyd
# res=aslrdd.map(lambda x:x.split(",")).filter(lambda x:int(x[1])==11)
'''op >>> ValueError: invalid literal for int() with base 10: 'age' >> beacause age column in have string format and "age" not get conv to int '''

#need to skip the line:
res=aslrdd.filter(lambda x:"age" not in x).map(lambda x:x.split(",")).filter(lambda x:int(x[1])==11)
for i in res.collect():
    print(i)