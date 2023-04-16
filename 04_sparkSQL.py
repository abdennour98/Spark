#!/usr/bin/env python
# coding: utf-8

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql import functions as F
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--input_green')
parser.add_argument('--input_yellow')
parser.add_argument('--output')
args=parser.parse_args()
input_green=args.input_green
input_yellow=args.input_yellow
output=args.output


spark=SparkSession.builder. \
    master("local[*]"). \
        appName("sparkSQL").\
            getOrCreate()


yellow_schema=types.StructType([
        types.StructField('VendorID', types.IntegerType(), True),\
        types.StructField('tpep_pickup_datetime', types.TimestampType(), True),\
        types.StructField('tpep_dropoff_datetime', types.TimestampType(), True),\
        types.StructField('passenger_count', types.IntegerType(), True),\
        types.StructField('trip_distance', types.DoubleType(), True),\
        types.StructField('RatecodeID', types.IntegerType(), True),\
        types.StructField('store_and_fwd_flag', types.StringType(), True),\
        types.StructField('PULocationID', types.IntegerType(), True),\
        types.StructField('DOLocationID', types.IntegerType(), True),\
        types.StructField('payment_type', types.IntegerType(), True),\
        types.StructField('fare_amount', types.DoubleType(), True),\
        types.StructField('extra', types.DoubleType(), True),\
        types.StructField('mta_tax', types.DoubleType(), True),\
        types.StructField('tip_amount', types.DoubleType(), True),\
        types.StructField('tolls_amount', types.DoubleType(), True),\
        types.StructField('improvement_surcharge', types.DoubleType(), True),\
        types.StructField('total_amount', types.DoubleType(), True),\
        types.StructField('congestion_surcharge', types.DoubleType(), True)])

df_yellow=spark.read.option('header','true').schema(yellow_schema).csv(input_yellow)
df_yellow.printSchema()
green_schema=types.StructType([    types.StructField('VendorID', types.IntegerType(), True),    types.StructField('lpep_pickup_datetime', types.TimestampType(), True),    types.StructField('lpep_dropoff_datetime', types.TimestampType(), True),    types.StructField('store_and_fwd_flag', types.StringType(), True),    types.StructField('RatecodeID', types.IntegerType(), True),    types.StructField('PULocationID', types.IntegerType(), True),    types.StructField('DOLocationID', types.IntegerType(), True),    types.StructField('passenger_count', types.IntegerType(), True),    types.StructField('trip_distance', types.DoubleType(), True),    types.StructField('fare_amount', types.DoubleType(), True),    types.StructField('extra', types.DoubleType(), True),    types.StructField('mta_tax', types.DoubleType(), True),    types.StructField('tip_amount', types.DoubleType(), True),    types.StructField('tolls_amount', types.DoubleType(), True),    types.StructField('ehail_fee', types.DoubleType(), True),    types.StructField('improvement_surcharge', types.DoubleType(), True),    types.StructField('total_amount', types.DoubleType(), True),    types.StructField('payment_type', types.IntegerType(), True),    types.StructField('trip_type', types.IntegerType(), True),    types.StructField('congestion_surcharge', types.DoubleType(), True)])

df_green=spark.read.option('header','true').schema(green_schema).csv(input_green)



df_green.schema



df_green.columns


df_green=df_green \
    .withColumnRenamed("lpep_pickup_datetime","pickup_datetime")\
        .withColumnRenamed("lpep_dropoff_datetime","dropoff_datetime")

df_yellow=df_yellow \
    .withColumnRenamed("tpep_pickup_datetime","pickup_datetime") \
        .withColumnRenamed("tpep_dropoff_datetime","dropoff_datetime")


common_columns=[col for col in df_green.columns if col in df_yellow.columns]



df_green_selected=df_green.select(common_columns)\
                   .withColumn('service_type',F.lit('green'))



df_yellow_selected=df_yellow.select(common_columns)\
    .withColumn('service_type',F.lit('yellow'))




df_trip_data=df_yellow_selected.unionAll(df_green_selected)





df_trip_data.printSchema()





df_trip_data.groupBy('service_type').count().show()





df_trip_data.registerTempTable('trips_data')




df_result=spark.sql("""
          select 
    -- Reveneue grouping 
    PULocationID as zone,
    date_trunc('month', pickup_datetime) as revenue_month, 
    --Note: For BQ use instead: date_trunc(pickup_datetime, month) as revenue_month, 
      
    service_type, 

    -- Revenue calculation 
    sum(fare_amount) as revenue_monthly_fare,
    sum(extra) as revenue_monthly_extra,
    sum(mta_tax) as revenue_monthly_mta_tax,
    sum(tip_amount) as revenue_monthly_tip_amount,
    sum(tolls_amount) as revenue_monthly_tolls_amount,
    sum(improvement_surcharge) as revenue_monthly_improvement_surcharge,
    sum(total_amount) as revenue_monthly_total_amount,
    sum(congestion_surcharge) as revenue_monthly_congestion_surcharge,

    -- Additional calculations
    avg(passenger_count) as avg_montly_passenger_count,
    avg(trip_distance) as avg_montly_trip_distance

    from trips_data
    group by 1,2,3
    """)







df_result.repartition(10).write.parquet(output,mode='overwrite')

