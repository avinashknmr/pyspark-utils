import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, LongType
from pyspark.testing.utils import assertDataFrameEqual, assertSchemaEqual

from pyspark_utils.session import SparkSessionManager
from pyspark_utils.data.transform import functions as cF
from pyspark_utils.data import transform as cT
from datetime import datetime

@pytest.fixture(scope="session")
def spark():
    spark_manager = SparkSessionManager(app_name='pytest-pyspark-local-testing')
    spark = spark_manager.create()
    yield spark
    spark_manager.stop()

@pytest.fixture(scope="module")
def original_df(spark):
    schema = StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("contacts", ArrayType(
            StructType([
                StructField("type", StringType(), True),
                StructField("value", StringType(), True)
            ])
        ), True)
    ])

    # Create sample data that matches the schema
    data = [
        (1, "Alice", [{"type": "email", "value": "alice@example.com"}, {"type": "phone", "value": "123-456-7890"}]),
        (2, "Bob", [{"type": "email", "value": "bob@example.com"}, {"type": "phone", "value": "234-567-8901"}]),
        (3, "Charlie", [{"type": "email", "value": "charlie@example.com"}])
    ]

    # Create a DataFrame
    df = spark.createDataFrame(data, schema)
    yield df

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

def test_year_frac(spark):
    original_df = spark.createDataFrame([(datetime(2023, 5, 1), datetime(2024, 6, 20))], ["from_dt", "to_dt"])
    expected_df = spark.createDataFrame([(datetime(2023, 5, 1), datetime(2024, 6, 20), 1.1344086025)], ["from_dt", "to_dt", "year_frac"])
    transformed_df = original_df.withColumn('year_frac', cF.year_frac('from_dt' , 'to_dt'))
    assertSchemaEqual(transformed_df.schema, expected_df.schema)
    assertDataFrameEqual(transformed_df, expected_df)

def test_flatten(spark, original_df):
    transformed_df = original_df.transform(cT.flatten)
    expected_data = [{'id': 1,
                    'name': 'Alice',
                    'contacts_type': 'email',
                    'contacts_value': 'alice@example.com'},
                    {'id': 1,
                    'name': 'Alice',
                    'contacts_type': 'phone',
                    'contacts_value': '123-456-7890'},
                    {'id': 2,
                    'name': 'Bob',
                    'contacts_type': 'email',
                    'contacts_value': 'bob@example.com'},
                    {'id': 2,
                    'name': 'Bob',
                    'contacts_type': 'phone',
                    'contacts_value': '234-567-8901'},
                    {'id': 3,
                    'name': 'Charlie',
                    'contacts_type': 'email',
                    'contacts_value': 'charlie@example.com'}]
    expected_df = spark.createDataFrame(expected_data)
    expected_df = expected_df.select('id', 'name', 'contacts_type', 'contacts_value')
    assertSchemaEqual(transformed_df.schema, expected_df.schema)
    assertDataFrameEqual(transformed_df, expected_df)

def test_pivot(spark, original_df):
    transformed_df = original_df.transform(cT.pivot, 'contacts', 'type')
    expected_data = [{'id': 1,
                    'name': 'Alice',
                    'email': 'alice@example.com',
                    'phone': '123-456-7890'},
                    {'id': 2, 'name': 'Bob', 'email': 'bob@example.com', 'phone': '234-567-8901'},
                    {'id': 3, 'name': 'Charlie', 'email': 'charlie@example.com', 'phone': None}]
    expected_df = spark.createDataFrame(expected_data)
    expected_df = expected_df.select('id', 'name', 'email', 'phone')
    assertSchemaEqual(transformed_df.schema, expected_df.schema)
    assertDataFrameEqual(transformed_df, expected_df)