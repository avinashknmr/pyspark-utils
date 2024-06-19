
class Sparksession:
    def __init__(self):
        self.start_time= datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        self.spark = (SparkSession.builder 
        .config("spark.jars.packages","io.delta:delta-core_2.12:2.4.0,"+"com.crealytics:spark-excel_2.12:3.2.1_0.17.0") 
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension" ) 
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") 
        .config("spark.databricks.delta.schema.autoMerge.enabled","true")
        .config("spark.sql.caseSensitive",True)  
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") 
        .config("spark.databricks.delta.vacuum.parallelDelete.enabled","true") 
        .config("spark.databricks.delta.merge.enableLowShuffle","true")  
        .config("spark.databricks.delta.optimizeWrite.enabled", "true") 
        .config("spark.databricks.delta.optimize.maxFileSize",104857600) 
        .config("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite","true")
        .config("spark.databricks.delta.compatibility.symlinkFormatManifest.enabled","true")
        .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
        .config("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY") 
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
        )
        logger.info("Class Sparksession initialized")
        
    def get_spark(self):
        return self.spark
    
    def read_excel(self,sheet_name:str,path:str)-> DataFrame:
        """
        read_excel function reads data only of excel files.

        :param sheet_name: Name of the sheet which you want to read
        :param path      : S3 path of the source file
        :return          : Dataframe if files exists   
        """
        try:
            dataframe= self.spark.read.format("com.crealytics.spark.excel") \
                    .option("header", "true") \
                    .option("inferSchema","false") \
                    .option("dataAddress",f"'{sheet_name}'!A0") \
                    .load(path)
        except:
            logger.warning(f"Sheet name or Excel path is incorrect - {sheet_name} : {path}")
            logger.error("IllegalArgumentException",exc_info=True)
            self.spark.stop()
            sys.exit()
        logger.info(f"Loaded Excel sheet [{sheet_name}] as Dataframe")
        return dataframe
        
    def get_start_time(self) -> str:
        """
        get_start_time function generates the start time ,which is used in update query
        on configs tables .

        :return     : String (Format: 2016-01-01T00:00:00 ) 
        """
        logger.info(f"Start time : {self.start_time}")
        return self.start_time


class Dataframe(Sparksession):
      
    def __init__(self):
        Sparksession.__init__(self)
        logger.info("Class Dataframe initialized")

    def read(self,src_path:str,modifiedafter:str,file_format:str,source="mongo")->DataFrame:
        """ 
        read function reads data from s3.

        :param src_path     : s3 path whiich you want to read
        :param modifiedafter: Time string , to read the files that are ingested
                                after the modifiedafter (Format: 2016-01-01T00:00:00 ) 
        :param file_format  : File format of the source file
        :return             : Raw Dataframe 
        """
        try:
            dataframe=self.spark.read.option("modifiedAfter",modifiedafter).option("recursiveFileLookup","true").format(file_format).load(f"{src_path}")
            logger.info(f"INPUT DATAFRAME COUNT FOR {src_path} : {dataframe.count()}")
        except Exception as e:
            logger.warning(f"Issue while reading from path : {src_path}")
            logger.error("IllegalArgumentException",exc_info=True)
            self.spark.stop()
            print(eval("errorfail"))
            sys.exit(1)
            raise e
        
        dataframe.printSchema()
        if source=="mongo" or source=="postgress":
            dataframe=dataframe.select("_airbyte_data.*")
        return dataframe


    def read_excel(self,sheet_name:str,path:str)-> DataFrame:
        """
        read_excel function reads data only of excel files.

        :param sheet_name: Name of the sheet which you want to read
        :param path      : S3 path of the source file
        :return          : Dataframe if files exists   
        """
        try:
            dataframe= self.spark.read.format("com.crealytics.spark.excel") \
                        .option("header", "true") \
                        .option("inferSchema","false") \
                        .option("dataAddress",f"'{sheet_name}'!A0") \
                        .load(path)
        except:
            logger.warning(f"Sheet name or Excel path is incorrect - {sheet_name} : {path}")
            logger.error("IllegalArgumentException",exc_info=True)
            self.spark.stop()
            sys.exit()
            logger.info(f"Loaded Excel sheet [{sheet_name}] as Dataframe")
        return dataframe
      