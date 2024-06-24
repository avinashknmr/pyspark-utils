from pyspark.sql import SparkSession
from threading import Lock
from .config.presets import required_jars

class SingleSparkSession:
    _instance = None
    # _lock = Lock()
    
    def __new__(cls, jars=None, additional_conf=None, *args, **kwargs):
        app_name = kwargs.get('app_name', 'myapp')
        # with cls._lock:
        print(cls._instance)
        if not cls._instance:
            cls._instance = super(SingleSparkSession, cls).__new__(cls)
            builder = SparkSession.builder.appName(app_name)
            if jars:
                builder = builder.config("spark.jars.packages", ",".join(required_jars(jars)))
            if additional_conf:
                for key, value in additional_conf.items():
                    builder.config(key, value)
            cls._instance.spark = builder.getOrCreate()
        return cls._instance
    
    @staticmethod
    def get_instance():
        return SingleSparkSession._instance.spark
    
    @staticmethod
    def stop():
        if SingleSparkSession._instance:
            SingleSparkSession._instance.spark.stop()
            SingleSparkSession._instance = None