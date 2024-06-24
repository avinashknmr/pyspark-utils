SCALA_VERSION = "2.12"
SPARK_VERSION = "3.5.0"
MONGO_SPARK_CONNECTOR_VERSION = "10.3.0"
CREALYTICS_EXCEL_VERSION = f"{SPARK_VERSION}_0.20.3"
DELTA_CORE_VERSION = "2.4.0"

def required_jars(req_jars:list=[]):
    jars = []
    jars_map = {
        "mongo": f"org.mongodb.spark:mongo-spark-connector_{SCALA_VERSION}:{MONGO_SPARK_CONNECTOR_VERSION}",
        "excel": f"com.crealytics:spark-excel_{SCALA_VERSION}:{CREALYTICS_EXCEL_VERSION}",
        "delta": f"io.delta:delta-core_{SCALA_VERSION}:{DELTA_CORE_VERSION}"
    }
    for rj in req_jars:
        if rj in jars_map.keys():
            jars.append(jars_map[rj])
    return jars

MONGO_CONFIGS = {
    "spark.sql.caseSensitive": True,
    "spark.mongodb.input.partitionerOptions.partitionSizeMB": "64", 
    "spark.mongodb.input.partitioner": "MongoSplitVectorPartitioner",
    "spark.speculation": "false",
    "spark.mongodb.read.outputExtendedJson": "false", 
    "spark.mongodb.write.convertJson": "true",
    "mapreduce.fileoutputcommitter.marksuccessfuljobs": "false", 
    "fs.file.impl.disable.cache": "true",
    "parquet.enable.summary-metadata": "false"
}