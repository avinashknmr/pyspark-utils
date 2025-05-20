# %%
import yaml
from pyspark_utils.session import SparkSessionManager
from pyspark_utils.config.presets import MONGO_CONFIGS, DELTA_CONFIGS

with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)
# %%
spark_manager = SparkSessionManager(app_name='pyspark-test', host='spark://localhost:7077', spark_config=MONGO_CONFIGS)
spark = spark_manager.create()
spark
# %%
spark.stop()
spark_manager.stop()
# %%
