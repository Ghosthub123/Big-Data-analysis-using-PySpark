import findspark
findspark.init()

import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('MySession').getOrCreate()

from pyspark.sql.functions import udf
func = udf(lambda data,x:'Accepted' not in data.select(x).collect())

df = spark.read.csv('C:/Users/dhana/OneDrive/Documents/Test.csv',header = True,inferSchema = True)

notaccepted= func(df,'Status')
notaccepted.show()
