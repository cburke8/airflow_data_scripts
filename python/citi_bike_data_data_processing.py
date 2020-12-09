import pandas as pd
import numpy as np
import os

bronze_path = 's3://citi-bike-batch-data-con/bronze/'
silver_path = 's3://citi-bike-batch-data-con/silver/'
gold_path   = 's3://citi-bike-batch-data-con/gold/'

# still working on this
s3_bucket = boto3.resource('s3')
bucket = s3.Bucket(bronze_path)
object_list = bucket.objects

for obj in object_list:
    print(obj)
    
BronzeDF = []

#drop duplicate rows
bronzeDF = bronzeDF.unique()
#order by trip start time
bronzeDF = bronzeDF.sort_values(by=['start_time'])
#view top of DF
bronzeDF.head()

# Still spark from here down
############################

bronzeDF.write.format('parquet').mode('overwrite').save(silver_path)

slvrDF = spark.read.format('parquet').load(silver_path)

#isolating circular trips
slvrDF.createOrReplaceTempView("silver")

# Making DIM_STATION starts:
# 2 start cols:
startDF = slvrDF.select(["start_station_id","start_station_name"]).distinct().withColumnRenamed("start_station_id","station_id").withColumnRenamed("start_station_name","station_name")
# 2 end cols:
endDF = slvrDF.select(["end_station_id","end_station_name"]).distinct().withColumnRenamed("end_station_id","station_id").withColumnRenamed("end_station_name","station_name")

# Merge them into dim station DF:
unionDF = startDF.unionAll(endDF).distinct()

#display(unionDF)
unionDF.write.format('parquet').mode('overwrite').save(f"{gold_path}dim_station")
