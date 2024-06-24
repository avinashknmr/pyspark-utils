import logging
from datetime import datetime, timezone
import boto3
from pyspark.sql.functions import (udf, col, regexp_replace, sha2,
                                   from_utc_timestamp, to_timestamp,
                                   from_unixtime, months_between, current_date)
from pyspark.sql.types import StringType, ArrayType, StructType, TimestampType, DateType
from pyspark.sql import SparkSession, DataFrame


logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)-8s | %(name)-6s | %(message)s', datefmt='%Y-%m-%d %I:%M:%S %p')
logger = logging.getLogger("CURATE")

def array_to_string(input_array):
    return regexp_replace(input_array.cast("string"), r'\[|\]|"', '')

@udf(returnType=TimestampType())
def udf_string_to_timestamp(date_str, date_format):
    return datetime.strptime(date_str, date_format)

@udf(returnType=TimestampType())
def udf_epoch_to_timestamp(epoch_seconds):
    return datetime.fromtimestamp(epoch_seconds)

def epoch_to_timestamp(epoch_seconds):
    return to_timestamp(from_unixtime(epoch_seconds))

def convert_timezone(timestamp, from_tz='UTC', to_tz='Asia/Kolkata'):
    if timestamp is None:
        return None
    utc_timestamp = from_utc_timestamp(to_timestamp(timestamp), from_tz)
    target_timestamp = from_utc_timestamp(utc_timestamp, to_tz)
    return target_timestamp

def utc_to_ist(timestamp):
    ist_timestamp = convert_timezone(timestamp, 'UTC', 'Asia/Kolkata')
    return ist_timestamp

@udf(returnType=TimestampType()) # to be tested
def udf_string_to_ist(date_str, date_format):
    timestamp = udf_string_to_timestamp(date_str, date_format="yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    ist_timestamp = utc_to_ist(timestamp)
    return ist_timestamp

def epoch_to_ist(epoch_seconds):
    timestamp = epoch_to_timestamp(epoch_seconds)
    ist_timestamp = utc_to_ist(timestamp)
    return ist_timestamp

def sha256_hash(input_str, bytes=256):
    return sha2(input_str, bytes)

# def year_frac(from_date, to_date=current_date()):
#     return months_between(to_date, from_date)/12