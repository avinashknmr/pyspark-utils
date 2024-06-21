from pyspark_utils.session import SparkSessionManager
from pyspark_utils.config.presets import MONGO_CONFIGS, DELTA_CONFIGS

spark_manager = SparkSessionManager(app_name='python-test', jars=['mongo'])
spark = spark_manager.create()

spark = spark_manager.restart(new_jars=['excel'], new_spark_config=MONGO_CONFIGS)
spark
spark = spark_manager.restart(new_jars=['excel'], overwrite=True)
