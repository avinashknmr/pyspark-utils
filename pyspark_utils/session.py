import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from threading import Lock
from .config.presets import required_jars
from loguru import logger
logger.add("logs/session.log", rotation="10 MB", retention="10 days", level="INFO")

class SparkSessionManager:
    def __init__(self, host='local[1]', app_name='MyApp', jars=[], spark_config={}):
        self.app_name = app_name
        self.host = host
        if not isinstance(jars, list):
            jars = [jars]
        self.jars = jars
        self.spark_config = spark_config
        if self.jars:
            self.spark_config.update({"spark.jars.packages": ",".join(required_jars(self.jars))})
        self.spark_session = None
        self.spark_context = None
        logger.debug('SparkSession Manager Initialized')

    def create(self):
        if self.spark_session is not None:
            raise Exception("SparkSession already exists. Please stop it first.")
        # Create SparkConf
        conf = SparkConf().setMaster(self.host).setAppName(self.app_name)

        for key, value in self.spark_config.items():
            conf.set(key, value)

        # Create SparkContext
        self.spark_context = SparkContext(conf=conf)

        # Create SparkSession
        self.spark_session = SparkSession.builder.config(conf=conf).getOrCreate()
        logger.info('SparkSession Started')
        return self.spark_session

    def stop(self):
        if self.spark_session is not None:
            self.spark_session.stop()
            self.spark_session = None
            logger.info('SparkSession Stopped')
        
        if self.spark_context is not None:
            self.spark_context.stop()
            self.spark_context = None
            logger.info('SparkSession Stopped')

    def restart(self, new_jars=[], new_spark_config={}, overwrite=False):
        self.stop()
        if not isinstance(new_jars, list):
            new_jars = [new_jars]
        if overwrite:
            self.spark_config = new_spark_config
            if new_jars:
                self.jars = new_jars
                self.spark_config.update({"spark.jars.packages": ",".join(required_jars(self.jars))})
                logger.info('SparkSession Restarted')
            
        else:
            if new_jars:
                self.jars.extend(new_jars)
                new_spark_config.update({"spark.jars.packages": ",".join(required_jars(self.jars))})
            self.spark_config.update(new_spark_config)
            logger.info('SparkSession Restarted')
        return self.create()