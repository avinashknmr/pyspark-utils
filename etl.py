import yaml
from pyspark_utils.session import SparkSessionManager

with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

spark_manager = SparkSessionManager(app_name='pyspark-test')
spark = spark_manager.create()