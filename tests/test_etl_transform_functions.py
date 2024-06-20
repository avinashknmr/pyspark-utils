import pytest
from pyspark.sql import SparkSession
from pyspark.testing.utils import assertDataFrameEqual
from pyspark_utils.etl.transform import functions as cF
from datetime import datetime

@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .appName("pytest-pyspark-local-testing") \
        .master("local[1]") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_epoch_to_timestamp(spark):
    original_df = spark.createDataFrame([(1718850938,)], ["epoch_time"])
    expected_df = spark.createDataFrame([(1718850938, datetime(2024, 6, 20, 8, 5, 38))], ["epoch_time", "timestamp"])
    transformed_df = original_df.withColumn('timestamp', cF.epoch_to_timestamp('epoch_time'))
    assertDataFrameEqual(transformed_df, expected_df)

def test_sha256_hash(spark):
    original_df = spark.createDataFrame([('pyspark',)], ["name"])
    expected_df = spark.createDataFrame([('pyspark', '91d17428d146fdbc804cb441daf689de97679bf6aee2ba4fb9e5920d7e174664')], ["name", "hashed"])
    transformed_df = original_df.withColumn('hashed', cF.sha256_hash('name'))
    assertDataFrameEqual(transformed_df, expected_df)