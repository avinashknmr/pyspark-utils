import logging
import base64
import json
from datetime import datetime, timezone
import boto3
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)-8s | %(name)-6s | %(message)s', datefmt='%Y-%m-%d %I:%M:%S %p')
logger = logging.getLogger("DWH-ETL")

SCALA_VERSION = "2.12"
SPARK_VERSION = "3.5.0"
MONGO_SPARK_CONNECTOR_VERSION = "10.3.0"
CREALYTICS_EXCEL_VERSION = f"{SPARK_VERSION}_0.20.3"
DELTA_CORE_VERSION = "2.4.0"

class MongoToS3Ingestion:
    def __init__(self):
        super().__init__()
        self.spark = (SparkSession.builder.appName("DWH-ETL")
            .config("spark.jars.packages",f"org.mongodb.spark:mongo-spark-connector_{SCALA_VERSION}:{MONGO_SPARK_CONNECTOR_VERSION}")
            .config("spark.sql.caseSensitive",True)
            .config("spark.mongodb.input.partitionerOptions.partitionSizeMB", "64") 
            .config("spark.mongodb.input.partitioner", "MongoSplitVectorPartitioner")
            .config("spark.speculation","false")
            .config("spark.mongodb.read.outputExtendedJson","false") 
            .config("spark.mongodb.write.convertJson","true")
            .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") 
            .config("fs.file.impl.disable.cache", "true")
            .config("parquet.enable.summary-metadata", "false")
            .getOrCreate()
        )
        logger.info("SparkSession initialized")

    def _build_url(self, creds, host, is_atlas=False, port=27017):
        username = creds['Username']
        password = creds['Password']
        if is_atlas:
            url = f"mongodb+srv://{username}:{password}@{host}/"
        else:
            url = f"mongodb://{username}:{password}@{host}:{port}/"
        return url

    def read(self, auth, host, database, collection, pipeline=[], port=27017, is_atlas=False, infer_schema="true", sample_pool_size=1000000, sample_size=100000):
        dataframe=(self.spark.read
            .format('mongodb')
            .option("inferSchema", infer_schema)
            .option("samplingRatio","1.0")
            .option("ssl.domain_match","false")
            .option("connection.uri", self._build_url(auth, host, is_atlas=is_atlas, port=port))
            .option("database", database)
            .option("collection", collection)
            .option("samplePoolSize",sample_pool_size)
            .option("sampleSize", sample_size)
            .option("aggregation.pipeline",pipeline)
            .load()
        )
        return dataframe 

    def write(self, data, path, mode="append", format="json"):
        rows = data.count() 
        n_partitions = rows//104857
        partitions=n_partitions+1
        logger.info(f"Total Dataframe Rows : {rows}")
        
        if data.isEmpty()==False:
            data.coalesce(partitions).write.mode(mode).format(format).save(path)
            logger.info("Data ingested to S3 in delta format.")
        else:
            logger.warning("No records in dataframe.")

    def stop(self):
        self.spark.stop()

class SourceToTargetMapper:
    def __init__(self):
        super().__init__()
        self.spark = (SparkSession.builder.appName("DWH-ETL")
            .config("spark.jars.packages",f"com.crealytics:spark-excel_{SCALA_VERSION}:{CREALYTICS_EXCEL_VERSION}") 
            .getOrCreate()
        )
        logger.info("SparkSession initialized")
    
    def read(self, file_path, sheet_name):
        try:
            dataframe= (self.spark.read
                .format("com.crealytics.spark.excel")
                .option("header", "true")
                .option("treatEmptyValuesAsNulls", "true")
                .option("inferSchema", "true")
                .option("sheetName", sheet_name)
                .load(file_path)
            )
            logger.info(f"Loaded excel sheet - {sheet_name} as dataframe")
            return dataframe
        except:
            logger.warning(f"Sheet name or excel path is incorrect - {file_path}: {sheet_name}")
            logger.error("IllegalArgumentException",exc_info=True)

    def write(self):
        pass

    def stop(self):
        self.spark.stop()

class DataProcessor:
    def __init__(self):
        super().__init__()
        self.spark = (SparkSession.builder.appName("DWH-ETL")
            .config("spark.jars.packages",f"io.delta:delta-core_{SCALA_VERSION}:{DELTA_CORE_VERSION}") 
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension" ) 
            .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") 
            .config("spark.databricks.delta.schema.autoMerge.enabled","true")
            .config("spark.sql.caseSensitive",True)  
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") 
            .config("spark.databricks.delta.vacuum.parallelDelete.enabled","true") 
            .config("spark.databricks.delta.merge.enableLowShuffle","true")  
            .config("spark.databricks.delta.optimizeWrite.enabled", "true") 
            .config("spark.databricks.delta.optimize.maxFileSize",104857600) 
            .config("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite","true")
            .config("spark.databricks.delta.compatibility.symlinkFormatManifest.enabled","true")
            .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
            .config("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
            .config("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY") 
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .getOrCreate()
        )
        logger.info("SparkSession initialized")
    
    def read(self):
        pass

    def write(self):
        pass
    
    def stop(self):
        self.spark.stop()