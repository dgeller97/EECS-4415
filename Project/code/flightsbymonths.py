from pyspark.sql import SparkSession

# create spark session
spark = SparkSession.builder.appName("flightAnalysisApp").getOrCreate()

# load parquet file from /user/root/ of "hdfs dfs" and create a view of it for spark.sql
# 1618_full_parquet is a parquet file of the flights from 2016 to 2018 inclusive
parquetFile = spark.read.parquet("1618_full_parquet")
parquetFile.createOrReplaceTempView("parquetFile")

# filter data in parquetFile by month
monthagg = spark.sql("SELECT SUBSTRING(FL_DATE, 6, 2) as FL_MONTH, DEP_DELAY, ARR_DELAY, LATE_AIRCRAFT_DELAY, WEATHER_DELAY, CRS_ELAPSED_TIME, ACTUAL_ELAPSED_TIME from parquetFile")
monthagg.createOrReplaceTempView("monthagg")

# average Delays per month
monthsavg = spark.sql("SELECT FL_MONTH, AVG(DEP_DELAY) as AVG_DEP_DELAY, \
	AVG(ARR_DELAY) as AVG_ARR_DELAY, \
	AVG(LATE_AIRCRAFT_DELAY) as AVG_LATE_AC_DELAY, \
	AVG(WEATHER_DELAY) as AVG_WEA_DELAY \
	from monthagg group by FL_MONTH order by FL_MONTH")

monthsavg.show()
monthsavg.coalesce(1).write.option("header","true").csv("/monthavg")

# Count # of delayed Flights by Month
delaycount = spark.sql("SELECT FL_MONTH, COUNT(FL_MONTH) as Delays FROM monthagg WHERE ACTUAL_ELAPSED_TIME > CRS_ELAPSED_TIME GROUP BY FL_MONTH SORT BY FL_MONTH")
delaycount.createOrReplaceTempView("delaycount")

# Count of Delayed and total flights per month, and the percentage of flights delayed.
avgcount = spark.sql("SELECT delaycount.FL_MONTH,Delays,Flights,cast(Delays/Flights as decimal(32,2)) as Percent_Late  FROM delaycount INNER JOIN (SELECT FL_MONTH, COUNT(FL_MONTH) as Flights FROM monthagg GROUP BY FL_MONTH) as tmp ON delaycount.FL_MONTH = tmp.FL_MONTH")

avgcount.show()
