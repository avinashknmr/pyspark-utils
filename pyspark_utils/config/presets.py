SCALA_VERSION = "2.12"
SPARK_VERSION = "3.5.2"
MONGO_SPARK_CONNECTOR_VERSION = "10.4.1"
CREALYTICS_EXCEL_VERSION = "3.5.1_0.20.4"
DELTA_SPARK_VERSION = "3.3.0"

def required_jars(req_jars:list=[]):
    jars = []
    jars_map = {
        "mongo": f"org.mongodb.spark:mongo-spark-connector_{SCALA_VERSION}:{MONGO_SPARK_CONNECTOR_VERSION}",
        "excel": f"com.crealytics:spark-excel_{SCALA_VERSION}:{CREALYTICS_EXCEL_VERSION}",
        "delta": f"io.delta:delta-spark_{SCALA_VERSION}:{DELTA_SPARK_VERSION}"
    }
    for rj in req_jars:
        if rj in jars_map.keys():
            jars.append(jars_map[rj])
    return jars

MONGO_CONFIGS = {
    "spark.sql.caseSensitive": "true",
    "spark.mongodb.input.partitionerOptions.partitionSizeMB": "64", 
    "spark.mongodb.input.partitioner": "MongoSplitVectorPartitioner",
    "spark.speculation": "false",
    "spark.mongodb.read.outputExtendedJson": "false", 
    "spark.mongodb.write.convertJson": "true",
    "mapreduce.fileoutputcommitter.marksuccessfuljobs": "false", 
    "fs.file.impl.disable.cache": "true",
    "parquet.enable.summary-metadata": "false"
}

DELTA_CONFIGS = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.databricks.delta.retentionDurationCheck.enabled": "false", 
    "spark.databricks.delta.schema.autoMerge.enabled": "true",
    "spark.sql.caseSensitive": "true",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.databricks.delta.vacuum.parallelDelete.enabled": "true",
    "spark.databricks.delta.merge.enableLowShuffle": "true",
    "spark.databricks.delta.optimizeWrite.enabled": "true",
    "spark.databricks.delta.optimize.maxFileSize": 104857600,
    "spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite": "true",
    "spark.databricks.delta.compatibility.symlinkFormatManifest.enabled": "true",
    "spark.sql.legacy.parquet.datetimeRebaseModeInWrite": "CORRECTED",
    "spark.sql.parquet.datetimeRebaseModeInRead": "LEGACY",
    "spark.sql.parquet.int96RebaseModeInWrite": "LEGACY",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",  
}