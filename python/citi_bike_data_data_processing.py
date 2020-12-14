import pandas as pd
import numpy as np
import io
import os
import boto3
import pyarrow

bucket_name = 'citi-bike-batch-data-test'
bucket_path = 's3://citi-bike-batch-data-test'
bronze_path = 's3://citi-bike-batch-data-test/bronze/'
silver_path = 's3://citi-bike-batch-data-test/silver/'
gold_path   = 's3://citi-bike-batch-data-test/gold/'

#def dataframe_to_s3(s3_client, input_datafame, bucket_name, filepath):
#    print("FP: ", filepath)
#    print("BN: ", bucket_name)
#    out_buffer = io.BytesIO()
#    input_datafame.to_parquet(out_buffer, index=False)
#    s3_client.put_object(Bucket=bucket_name, Key=filepath, Body=out_buffer.getvalue())

header_list = ['tripduration', 'start_time', 'stop_time','start_station_id', 'start_station_name',
               'start_station_latitude', 'start_station_longitude', 'end_station_id', 'end_station_name', 
               'end_station_latitude', 'end_station_longitude','bike_id', 'user_type', 'birth_year', 'user_gender']

dtypes_dict = {'tripduration': 'str', 'start_time': 'str', 'stop_time': 'str',
          'start_station_id': 'str', 'start_station_name': 'str',
          'start_station_latitude': 'str', 'start_station_longitude': 'str', 
          'end_station_id': 'str', 'end_station_name': 'str', 
          'end_station_latitude': 'str', 'end_station_longitude': 'str',
          'bike_id': 'str', 'user_type': 'str', 
          'birth_year': 'str', 'user_gender': 'str'}

parse_dates_list = ['tripduration', 'start_time', 'stop_time']

bronze_df = []

def read_bronze_csv(s3_object, s3_client, io_out_buffer):
    if '.csv' in obj['Key']:
        temp_df = []
        temp_folder_name = (obj['Key']).split('/')[0]
        temp_file_name = (obj['Key']).split('/')[1]
        temp_csv_path = bucket_path + "/" + temp_folder_name + "/" + temp_file_name
        
        temp_df = pd.read_csv(temp_csv_path, names = header_list, dtype = dtypes_dict, parse_dates = parse_dates_list)
                   
        #drop duplicate rows
        temp_df = temp_df.drop_duplicates()
        #order by trip start time
        temp_df = temp_df.sort_values(by=['start_time'])
        
        temp_df.to_parquet(io_out_buffer, index=False)
        s3_client.put_object(Bucket=bucket_name, Key=('silver/' + temp_file_name.split('.csv')[0] + '.parquet'), Body=out_buffer.getvalue())
        print("one file load to s3 complete")
        
s3 = boto3.client('s3')
s3_object_list = s3.list_objects_v2(Bucket=bucket_name, Prefix="bronze/")['Contents']
print("OL length: ", len(s3_object_list))

out_buffer = io.BytesIO()

for obj in s3_object_list:
    print("reading object and loading to silver")
    read_bronze_csv(obj, s3, out_buffer)

# Still spark from here down
############################

#bronzeDF.write.format('parquet').mode('overwrite').save(silver_path) #completed above

#slvrDF = spark.read.format('parquet').load(silver_path) #completed above

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

