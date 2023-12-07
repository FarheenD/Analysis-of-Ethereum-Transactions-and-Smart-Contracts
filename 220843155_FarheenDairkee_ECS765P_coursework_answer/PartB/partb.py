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
#import matplotlib


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
    
    def good_line_c(line):
        try:
            fields = line.split(',')
            if len(fields)<=4:
                return False
            else:
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
        
    trans_to_address=clean_transactions.map(lambda l: (l.split(',')[6],int(l.split(',')[7])))
    trans_to_address=trans_to_address.reduceByKey(operator.add)
    print(trans_to_address.take(2))
    #[('0x2cf3cf3c9fd3dccd6fa96e495046b74fafbdf838', 6888977220000000000), ('0x01d1d9b03e7315a9594c5682bc9d17b3e5a824bf', 46287694167912)]
    
    contracts = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/contracts.csv")
    clean_contracts = contracts.filter(good_line_c)
    contract_address=clean_contracts.map(lambda l:(l.split(',')[0],1))
    Ether_Data=trans_to_address.join(contract_address)

    print(Ether_Data.take(2))
    #[('0x782c4adfab128f9d9475d3403e575d635c95e333', (6100320400000000000, 1)), ('0xe9396ba86bf3fa883e62e3ba43784414624cbc33', (3200000000000000, 1))]
    
    final_Ether_Data=Ether_Data.map(lambda x: (x[0],x[1][0]))  
    top10=final_Ether_Data.takeOrdered(10, key=lambda x: -x[1])
    
    
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum_' + date_time + '/top10_smart_contracts.txt')
    my_result_object.put(Body=json.dumps(top10))

    spark.stop()
