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
    
    def good_line_t(line):
        try:
            fields = line.split(',')
            if len(fields)!=15:
                return False
            #int(fields[0])
            int(fields[7])
            #int(fields[4])
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

    transactions = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    clean_transactions = transactions.filter(good_line_t) 
        
    
    transaction_count = clean_transactions.map(lambda b: (time.strftime("%Y-%m",time.gmtime(int(b.split(',')[11]))),1))
    trans_records=transaction_count.count()
    transaction_count=transaction_count.reduceByKey(operator.add)
    print(transaction_count.take(2))
    #[["2017-06", 7244657], ["2018-03", 20261862]]
    
    transaction_count2 = clean_transactions.map(lambda b: (time.strftime("%Y-%m",time.gmtime(int(b.split(',')[11]))),int(b.split(',')[7])))
    trans_records=transaction_count2.count()
    transaction_count2=transaction_count2.reduceByKey(operator.add)
    transaction_avg=transaction_count2.map(lambda x:(x[0],x[1]/trans_records))
    print(transaction_avg.take(2)) 
    #[[('2017-06', 1.1124614922553339e+18), ('2018-03', 1.494683174922976e+17)]]
                                              
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")
    

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
    
    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum_' + date_time + '/transaction_Count.txt')
    my_result_object.put(Body=json.dumps(transaction_count.take(100)))
    
    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum_' + date_time + '/transaction_Average.txt')
     my_result_object.put(Body=json.dumps(transaction_avg.take(100)))
    

    spark.stop()
