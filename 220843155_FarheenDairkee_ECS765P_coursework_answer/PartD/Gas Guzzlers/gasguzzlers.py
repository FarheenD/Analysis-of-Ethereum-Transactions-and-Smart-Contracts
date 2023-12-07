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
            elif fields:
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
        
    trans_gas_price=clean_transactions.map(lambda l: (time.strftime("%Y-%m",time.gmtime(int(l.split(',')[11]))),(int(l.split(',')[9]),1)))
    Gas_Price_Data=trans_gas_price.map(lambda x: ((x[0]),(x[1][0],x[1][1])))
    gas_price_over_time=Gas_Price_Data.reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1]),)
    gas_price_over_time_final=gas_price_over_time.map(lambda x:"{},{},{}".format(x[0],x[1][0],int(x[1][0])/x[1][1]))
    print(gas_price_over_time_final.take(2))
    
    #['2017-06,218784602251092080,30199442465.128727', '2018-03,315067212049139490,15549765961.743273']
    rdd_string1 = '\n'.join(gas_price_over_time_final.collect())
    
    transactions = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    clean_transactions = transactions.filter(good_line_t)
    trans_gas=clean_transactions.map(lambda l: (l.split(',')[6],(time.strftime("%Y-%m",time.gmtime(int(l.split(',')[11]))),int(l.split(',')[8]))))
    
    average_gas=trans_gas=clean_transactions.map(lambda l: ('avg',(int(l.split(',')[8]),1)))
    average_gas=average_gas.reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1]),)
    average_gas_final=average_gas.map(lambda x:"{}:{}".format(x[0],int(x[1][0])/x[1][1]))                                            
    print(average_gas_final.take(1))
    #['avg:161725.69962368018']
    
    
    contracts = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/contracts.csv")
    clean_contracts = contracts.filter(good_line_c)
    contract_data=clean_contracts.map(lambda l:(l.split(',')[0],1))
    Contract_Gas_used_Data=trans_gas.join(contract_data)
    print(Contract_Gas_used_Data.take(2))
    #[('0x4edeadf02843c1565fdb85e7757d6b91eebaab5f', (('2016-10', 0), 1)), ('0x4edeadf02843c1565fdb85e7757d6b91eebaab5f', (('2017-07', 3467920), 1))]
    
    Gas_used_data=Contract_Gas_used_Data.map(lambda x: (x[1][0][0],(x[1][0][1],x[1][1])))
    gas_used_over_time=Gas_used_data.reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1]),)
    gas_used_over_time_final=gas_used_over_time.map(lambda x:"{},{},{}".format(x[0],x[1][0],int(x[1][0])/x[1][1]))
    
    Gas_used_data2=Contract_Gas_used_Data.map(lambda x: (x[0],(x[1][0][1],x[1][1])))
    gas_used_contracts=Gas_used_data2.reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1]),)
    gas_used_over_contracts_final=gas_used_contracts.map(lambda x:"{},{},{}".format(x[0],x[1][0],int(x[1][0])/x[1][1]))
    rdd_string2 = '\n'.join(gas_used_over_contracts_final.collect())
    
    
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")
    
    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
    
    
    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum_' + date_time + '/gas_price_over_time.txt')
    my_result_object.put(Body=rdd_string1)
    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum_' + date_time + '/gas_used_over_time_final.txt')
    my_result_object.put(Body=json.dumps(gas_used_over_time_final.take(100))
    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum_' + date_time + '/gas_used_over_time_contracts (1).txt')
    my_result_object.put(Body=rdd_string2)
    
    spark.stop()
