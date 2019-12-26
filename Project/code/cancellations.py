#NOTE: preliminary work only

from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import *
import pyspark.sql.functions as F
from pyspark.sql.types import *
import sys

conf = SparkConf()
conf.setAppName("Cancellations")
sc = SparkContext(conf=conf)
sc.setLogLevel("Error")

spark = SparkSession \
    .builder \
    .appName("flightdata") \
    .getOrCreate()

#reads parquet file
pf = spark.read.parquet("/2016_full_parquet")

pf.groupBy("CANCELLATION_CODE").count().show()

#total cancelled
pf.groupBy("CANCELLED").count().show()

#finds delays  (10 mins or more)
delay10 = pf.filter(pf['DEP_DELAY'] > 10).count()

print("flights delayed: " + str(delay10))

#finds delays due to carrier
car10 = pf.filter(pf['CARRIER_DELAY'] > 10).count()
print("carrier delay: " + str(car10))

#finds delays due to weather
we10 = pf.filter(pf['WEATHER_DELAY'] > 10).count()
print("weather delay: " + str(we10))

#finds delays due to security
sec10 = pf.filter(pf['SECURITY_DELAY'] > 10).count()
print("security delay: " + str(sec10))

#finds delays due to late aircraft
late10 = pf.filter(pf['LATE_AIRCRAFT_DELAY'] > 10).count()
print("late aircraft delay: " + str(late10))

#finds # of flights delayed at airport
print("Airport # of flights delayed:")
pf.filter(pf['DEP_DELAY'] > 10).groupBy("DEST").count().orderBy(["count"], ascending = [0]).show()

#finds total # of flights delayed at airport
print("Airport # of flights total:")
pf.groupBy("DEST").count().orderBy(["count"], ascending = [0]).show()

#finds # of flights delayed by carrier
print("Carrier # of flights delayed:")
pf.filter(pf['DEP_DELAY'] > 10).groupBy("OP_UNIQUE_CARRIER").count().orderBy(["count"], ascending = [0]).show()

#finds total # of flights delayed by carrier
print("Carrier # of flights total:")
pf.groupBy("OP_UNIQUE_CARRIER").count().show()

