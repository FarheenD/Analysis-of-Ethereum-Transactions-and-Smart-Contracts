import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime
from time import gmtime, strftime


if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("ethereum-parvulus")\
        .getOrCreate()
    
        
    def good_line_b(line):
        try:
            fields = line.split(',')
            if len(fields)!=19:
                return False
            int(fields[12])
            return True
        except:
            return False
    
    # shared read-only object bucket containing datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']

    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")

       
    blocks=spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/blocks.csv")
    clean_blocks=blocks.filter(good_line_b)
    sorted_counts = clean_blocks.map(lambda l : (l.split(',')[9],int(l.split(',')[12])))
    counts=sorted_counts.reduceByKey(lambda a,b : a+b).sortBy(lambda x: x[1], False)
    
    
    i=0
    for miner,size in counts.collect()[0:10]:
        print("{} : {} : {} ".format(i,miner,size))
        i += 1
    
    
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")
    

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
    
    
    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum_' + date_time + '/top10_miners.txt')
    my_result_object.put(Body=json.dumps(counts.take(10)))
    
    

    spark.stop()
