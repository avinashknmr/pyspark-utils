import logging
import base64
import json
from datetime import datetime, timezone
import boto3
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)-8s | %(name)-6s | %(message)s', datefmt='%Y-%m-%d %I:%M:%S %p')
logger = logging.getLogger("INGEST")

class SparkMongoReader:
    def __init__(self):
        self.start_time= datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4]+'Z'
        self.spark =  (SparkSession.builder 
        .config("spark.jars.packages","org.mongodb.spark:mongo-spark-connector_2.12:10.2.0")
        .config("spark.sql.caseSensitive",True)  
        .config("spark.mongodb.input.partitionerOptions.partitionSizeMB", "64") 
        .config("spark.mongodb.input.partitioner", "MongoSplitVectorPartitioner")
        .config("spark.speculation","false")
        .config("spark.mongodb.read.outputExtendedJson","false") 
        .config("spark.mongodb.write.convertJson","true") 
        .getOrCreate()
        )
        logger.info("Class Sparksession initialized")
        
    def get_spark(self):
        return self.spark
        
    def get_secret(self,secret_name):
        region_name = "ap-south-1"
    

        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name)
    
        get_secret_value_response = client.get_secret_value(
                            SecretId=secret_name
                        )
        if 'SecretString' in get_secret_value_response:
            decoded_secret = get_secret_value_response['SecretString']
        else:
            decoded_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
        return json.loads(decoded_secret) 
        
    def generate_raw_path(self,source,database,collection):
        now = datetime.now()
        year = now.year
        month = now.month 
        day = now.day
        hour = now.hour
        path = (
                f"s3a://xxxx-de-beta-xy/raw/{source}/{database.lower()}/{collection.lower()}/"
                + "/ingest_year="
                + "{:0>4}".format(str(year))
                + "/ingest_month="
                + "{:0>2}".format(str(month))
                + "/ingest_day="
                + "{:0>2}".format(str(day))
                + "/ingest_hour="
                + "{:0>2}".format(str(hour))
                + "/"
            )
        return path
        
        
    def get_start_time(self) -> str:

        logger.info(f"Start time : {self.start_time}")
        return self.start_time
        
    def read_mongo_xy(self,database,collection,pipeline):
        creds=self.get_secret("arn:aws:secretsmanager:ap-south-1:859XXXXXXX55:secret:beta/de/xy/Mongo-3MwFDp")
        
        
        dataframe=self.spark.read\
                .option("inferSchema","true") \
                .format('mongodb')\
                .option("samplingRatio","1.0")  \
                .option("ssl.domain_match","false") \
                .option("connection.uri",f"mongodb+srv://{creds['Username']}:{creds[' Password']}@beta-xy-pl-3.xxxxx.mongodb.net/") \
                .option("database", database) \
                .option("collection", collection) \
                .option("samplePoolSize",1000000)  \
                .option("sampleSize", 100000) \
                .option("aggregation.pipeline",pipeline) \
                .load() 

        return dataframe 
        
    def write_to_s3(self,df,path):
        
        rows = df.count() 
        n_partitions = rows//104857
        partitions=n_partitions+1
        logger.info(f"TOTAL DATAFRAME ROWS : {rows}")
        
        if df.isEmpty()==False:
            df.coalesce(partitions).write \
                    .mode("append")   \
                    .json(path)
            logger.info("INGESTION DATA TO S3 ")
        else:
            logger.warning("NO RECORDS FOUND IN DATAFRAME")


class CustomSparkSession:
    def __init__(self):
        pass

    def reader(self):
        pass

    def writer(self):
        pass

class SparkMongo(CustomSparkSession):
    def __init__(self):
        pass

    def reader(self):
        pass

    def writer(self):
        pass

class SparkExcel(CustomSparkSession):
    def __init__(self):
        pass
    
    def reader(self):
        pass

    def writer(self):
        pass

class S3Delta(CustomSparkSession):
    def __init__(self):
        pass
    
    def reader(self):
        pass

    def writer(self):
        pass