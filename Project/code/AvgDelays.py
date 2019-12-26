from pyspark.sql import SparkSession

# create spark session
spark = SparkSession.builder.appName("flightAnalysisApp").getOrCreate()

# load parquet file from /user/root/ of "hdfs dfs" and create a view of it for spark.sql
# 1618_full_parquet is a parquet file of the flights from 2016 to 2018 inclusive
parquetFile = spark.read.parquet("1618_full_parquet")
parquetFile.createOrReplaceTempView("parquetFile") 

# sql query of parquetFile for the routes and average delay of the routes counting only delays and routes with more than 1000 flights
# for average delay of all flights simply remove "(ACTUAL_ELAPSED_TIME - CRS_ELAPSED_TIME) > 0" from where
routes = spark.sql("select * from (select ORIGIN, DEST, count(FL_DATE) as COUNT, cast(avg(ACTUAL_ELAPSED_TIME - CRS_ELAPSED_TIME) as decimal(32,2)) as AVG from parquetFile where CANCELLED != 1 and (ACTUAL_ELAPSED_TIME - CRS_ELAPSED_TIME) > 0 group by ORIGIN, DEST order by AVG) where COUNT >= 1000")
# write as csv to "hdfs dfs /" for further analysis and graphing
routes.coalesce(1).write.option("header","true").csv("/routesAvgFiltered")

# query # of flights by month and day
dates = spark.sql("select MONTH, DAY, count(DAY) as COUNT, cast(avg(AET - CET) as decimal(32,2)) as AVG from \
			(select substring(FL_DATE, 6, 2) as MONTH, substring(FL_DATE, 9, 2) as DAY, ACTUAL_ELAPSED_TIME as AET, CRS_ELAPSED_TIME as CET from parquetFile where CANCELLED != 1) \
			group by MONTH, DAY order by MONTH, DAY")
dates.coalesce(1).write.option("header","true").csv("/datesAvg")

# query # of flights by day of the week
dayOfWeek = spark.sql("select date_format(FL_DATE, 'E') as DayOfWeek, count(FL_DATE) as COUNT, cast(avg(ACTUAL_ELAPSED_TIME - CRS_ELAPSED_TIME) as decimal(32,2)) as AVG from parquetFile \
			where CANCELLED != 1 group by DayOfWeek")
dayOfWeek.coalesce(1).write.option("header","true").csv("/dayOfWeekAvg")

# query # of flights and average delay of flights by Carrier. Count only delayed flights.
# To count all remove (ACTUAL_ELAPSED_TIME - CRS_ELAPSED_TIME) > 0.
carriers = spark.sql("select OP_UNIQUE_CARRIER as Carrier, count(FL_DATE), cast(avg(ACTUAL_ELAPSED_TIME - CRS_ELAPSED_TIME) as decimal(32,2)) as AVG from parquetFile where CANCELLED != 1 and (ACTUAL_ELAPSED_TIME - CRS_ELAPSED_TIME) > 0 group by \
			OP_UNIQUE_CARRIER")
carriers.coalesce(1).write.option("header","true").csv("/carriersAvgFiltered")

# query departure delays by airport for only delayed departures and airports that see more than 1000 flights.
depAvg = spark.sql("select * from ( \
			select ORIGIN, ORIGIN_CITY_NAME, count(*) as COUNT, cast(avg(DEP_DELAY) as decimal(32,2)) as AVG from parquetFile where CANCELLED != 1  and DEP_DELAY > 0 \
			group by ORIGIN, ORIGIN_CITY_NAME ) where COUNT > 1000")
depAvg.coalesce(1).write.option("header","true").csv("/depAvgFiltered")

# used to check structure of tables.
routes.show(truncate = False)
dates.show(truncate = False)
dayOfWeek.show(truncate = False)
carriers.show(truncate = False)
depAvg.show(truncate = False)
