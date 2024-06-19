import logging
from datetime import datetime, timezone, from_utc_timestamp, to_timestamp
import boto3
from pyspark.sql.functions import udf, col, regexp_replace, sha2
from pyspark.sql.types import StringType, ArrayType, StructType, TimestampType, DateType
from pyspark.sql import SparkSession, DataFrame


logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)-8s | %(name)-6s | %(message)s', datefmt='%Y-%m-%d %I:%M:%S %p')
logger = logging.getLogger("CURATE")

@udf(returnType=StringType())
def array_to_string(input_array):
    return regexp_replace(input_array.cast("string"), r'\[|\]|"', '')

@udf(returnType=TimestampType())
def string_to_timestamp(date_str, date_format):
    return datetime.strptime(date_str, date_format)

@udf(returnType=TimestampType())
def epoch_to_timestamp(epoch_seconds):
    return datetime.fromtimestamp(epoch_seconds)

@udf(returnType=TimestampType())
def convert_timezone(timestamp, from_tz='UTC', to_tz='Asia/Kolkata'):
    if timestamp is None:
        return None
    utc_timestamp = from_utc_timestamp(to_timestamp(timestamp), from_tz)
    target_timestamp = from_utc_timestamp(utc_timestamp, to_tz)
    return target_timestamp

@udf(returnType=TimestampType())
def utc_to_ist(timestamp):
    ist_timestamp = convert_timezone(timestamp, 'UTC', 'Asia/Kolkata')
    return ist_timestamp

@udf(returnType=TimestampType())
def string_to_ist(date_str, date_format):
    timestamp = string_to_timestamp(date_str, date_format="yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    ist_timestamp = utc_to_ist(timestamp)
    return ist_timestamp

@udf(returnType=TimestampType())
def epoch_to_ist(epoch_seconds):
    timestamp = epoch_to_timestamp(epoch_seconds)
    ist_timestamp = utc_to_ist(timestamp)
    return ist_timestamp

@udf(returnType=StringType())
def sha256_hash(input_str, bytes=256):
    return sha2(input_str, bytes)