from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
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

class SparkSessionManager:
    def __init__(self, app_name, jars=[], spark_config={}):
        self.app_name = app_name
        self.jars = jars
        self.spark_config = spark_config
        if self.jars:
            self.spark_config.update({"spark.jars.packages": ",".join(required_jars(self.jars))})
        self.spark_session = None
        self.spark_context = None

    def create(self):
        if self.spark_session is not None:
            raise Exception("SparkSession already exists. Please stop it first.")
        # Create SparkConf
        conf = SparkConf().setAppName(self.app_name)

        for key, value in self.spark_config.items():
            conf.set(key, value)

        # Create SparkContext
        self.spark_context = SparkContext(conf=conf)

        # Create SparkSession
        self.spark_session = SparkSession.builder.config(conf=conf).getOrCreate()

        return self.spark_session

    def stop(self):
        if self.spark_session is not None:
            self.spark_session.stop()
            self.spark_session = None
        
        if self.spark_context is not None:
            self.spark_context.stop()
            self.spark_context = None

    def restart(self, new_jars=[], new_spark_config={}, overwrite=False):
        self.stop()
        if overwrite:
            self.spark_config = new_spark_config
            if new_jars:
                self.jars = new_jars
                self.spark_config.update({"spark.jars.packages": ",".join(required_jars(self.jars))})
            
        else:
            if new_jars:
                self.jars.extend(new_jars)
                new_spark_config.update({"spark.jars.packages": ",".join(required_jars(self.jars))})
            self.spark_config.update(new_spark_config)
        return self.create()