import sys
import logging
from pyspark.sql import SparkSession
from collections import Counter
import sys,boto3,re,json,base64
import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.window as psw
import pyspark.sql.functions as psf
from pyspark.sql.dataframe import DataFrame
from datetime import datetime
from multiprocessing.pool import ThreadPool
from datetime import datetime,timezone


log_format = logging.Formatter('[%(asctime)s] [%(levelname)s] - %(message)s')
log = logging.getLogger("test")                                  
log.setLevel(logging.INFO)                                       
                                                   
handler = logging.StreamHandler()                             
handler.setLevel(logging.INFO)                                        
handler.setFormatter(log_format)                                        
log.addHandler(handler) 
now = datetime.now().strftime("%Y-%m-%d")
#log.disabled = False


def get_input_arguments():
    ia={}
    if len(sys.argv)>1:
           for key,value in eval(sys.argv[1]).items():
                ia[key]=value
                log.info(f"Input Arguments passed - {key} : {value}")
        
    else:
            log.info("Input Arguments passed")
    return ia
       
     
def log_info(messege):
         log.info(messege)


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
            .config ("spark.databricks.delta.optimizeWrite.enabled", "true") 
            .config("spark.databricks.delta.optimize.maxFileSize",104857600) 
            .config("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite","true")
            .config("spark.databricks.delta.compatibility.symlinkFormatManifest.enabled","true")
            .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
            .config("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
            .config("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY") 
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .getOrCreate()
            )
            log.info("Class Sparksession initialized")
            
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
                    log.warning(f"Sheet name or Excel path is incorrect - {sheet_name} : {path}")
                    log.error("IllegalArgumentException",exc_info=True)
                    self.spark.stop()
                    sys.exit()
                log.info(f"Loaded Excel sheet [{sheet_name}] as Dataframe")
                return dataframe
            
        def get_start_time(self) -> str:
            """
            get_start_time function generates the start time ,which is used in update query
            on configs tables .

            :return     : String (Format: 2016-01-01T00:00:00 ) 
            """
            log.info(f"Start time : {self.start_time}")
            return self.start_time
  

class Queryconfig:
        def __init__(self):
                self.database="configs"
                self.raw_config="raw"
                self.curated_config="curated"
                self.bi_config="analytics"
                self.bucket= "xxxx-de-beta-data"
                self.path= "athena_query_output"
                self.workgroup="ETL"
                self.client=boto3.client('athena', region_name='ap-south-1')
                log.info("Class Queryconfig initialized")
            
        def run_athena_query(self,query:str)-> dict:

            """
            run_athena_query function runs a SQL query on Athena .

            :param query: sql query
            :return     : Output of SQL query 
            """
            retry=True
            log.info(f"Query: {query}")
            response = self.client.start_query_execution(
                                        QueryString=query,
                                        QueryExecutionContext={'Database': self.database},
                                        ResultConfiguration={'OutputLocation': 's3://' + self.bucket + '/' + self.path},
                                        WorkGroup=self.workgroup
                                      )
            queryid=response['QueryExecutionId']
            status=self.client.get_query_execution(QueryExecutionId=queryid) 
            state='QUEUED'
            while state =='QUEUED' or state =='RUNNING':
                status=self.client.get_query_execution(QueryExecutionId=queryid) 
                state=status['QueryExecution']['Status']['State']
            
            log.info(f"Query status : {state}")
            if state=='FAILED' and retry==False:
                        log.warning("Query failed - Please check the Config table ")
                        log.error("IllegalArgumentException",exc_info=True)
                        sys.exit(1)

            elif state=='FAILED' and retry==True :
                self.run_athena_query(query)
                retry=False
            else:
                results=self.client.get_query_results(QueryExecutionId=queryid)
                return results
        
        
        def get_config_metadata(self,query:str,config_table:str)->dict:
            """
            get_config_metadata function is used create a query excecution on configs 
            tables and returns metadata,
            If job is for first load / incremental load gets decided by the value of last_modified.
            If first load : created_at - current_timestamp else no need to update the created at field
                    
            :return     : dictionary that contains configs tables metadata
            """
            
            details={}
            response=self.run_athena_query(query)
            for val in range(len(response['ResultSet']['Rows'][0]['Data'])):
                details[response['ResultSet']['Rows'][0]['Data'][val]['VarCharValue']]=response['ResultSet']['Rows'][1]['Data'][val]['VarCharValue']
                log.info(f"{response['ResultSet']['Rows'][0]['Data'][val]['VarCharValue']}: {response['ResultSet']['Rows'][1]['Data'][val]['VarCharValue']}")
            if config_table!="raw":
                details['first_load']=True if details['last_modified']=='2016-01-01T00:00:00' else False
                log.info(f"First Load : {details['first_load']}")
                details['created_at']=',created_at=current_timestamp' if details['last_modified']=='2016-01-01T00:00:00' else ''
            return details
class MongoSparksession:
        def __init__(self):
            self.start_time= datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4]+'Z'
            self.spark =  (SparkSession.builder 
            .config("spark.jars.packages","org.mongodb.spark:mongo-spark-connector_2.12:10.2.0")
            .config("spark.sql.caseSensitive",True)  
            .config("spark.mongodb.input.partitionerOptions.partitionSizeMB", "64") 
            .config("spark.mongodb.input.partitioner", "MongoSplitVectorPartitioner")
            .config("spark.speculation","false")
            .config("spark.mongodb.read.outputExtendedJson","false") 
            .config("spark.mongodb.write.convertJson","true") 
            .getOrCreate()
            )
            log.info("Class Sparksession initialized")
            
        def get_spark(self):
            return self.spark
            
        def get_secret(self,secret_name):
                    region_name = "ap-south-1"
                
        
                    session = boto3.session.Session()
                    client = session.client(
                        service_name='secretsmanager',
                        region_name=region_name)
                
                    get_secret_value_response = client.get_secret_value(
                                        SecretId=secret_name
                                    )
                    if 'SecretString' in get_secret_value_response:
                        decoded_secret = get_secret_value_response['SecretString']
                    else:
                        decoded_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
                    return json.loads(decoded_secret) 
            
        def generate_raw_path(self,source,database,collection):
            now = datetime.now()
            year = now.year
            month = now.month 
            day = now.day
            hour = now.hour
            path = (
                    f"s3a://xxxx-de-beta-xy/raw/{source}/{database.lower()}/{collection.lower()}/"
                    + "/ingest_year="
                    + "{:0>4}".format(str(year))
                    + "/ingest_month="
                    + "{:0>2}".format(str(month))
                    + "/ingest_day="
                    + "{:0>2}".format(str(day))
                    + "/ingest_hour="
                    + "{:0>2}".format(str(hour))
                    + "/"
                )
            return path
            
            
        def get_start_time(self) -> str:

            log.info(f"Start time : {self.start_time}")
            return self.start_time
            
        def read_mongo_xy(self,database,collection,pipeline):
            creds=self.get_secret("arn:aws:secretsmanager:ap-south-1:85XXXXXXXXX55:secret:beta/de/xy/xxxxx-3MwFDp")
           
            
            dataframe=self.spark.read\
                    .option("inferSchema","true") \
                    .format('mongodb')\
                    .option("samplingRatio","1.0")  \
                    .option("ssl.domain_match","false") \
                    .option("connection.uri",f"mongodb+srv://{creds['Username']}:{creds[' Password']}@xxxxxxxxxxxxxxxx.mongodb.net/") \
                    .option("database", database) \
                    .option("collection", collection) \
                    .option("samplePoolSize",1000000)  \
                    .option("sampleSize", 100000) \
                    .option("aggregation.pipeline",pipeline) \
                    .load() 
    
            return dataframe 
            
        def write_to_s3(self,df,path):
            
            rows = df.count() 
            n_partitions = rows//104857
            partitions=n_partitions+1
            log.info(f"TOTAL DATAFRAME ROWS : {rows}")
            
            if df.isEmpty()==False:
                df.coalesce(partitions).write \
                        .mode("append")   \
                        .json(path)
                log.info("INGESTION DATA TO S3 ")
            else:
                log.warning("NO RECORDS FOUND IN DATAFRAME")
           
class Dataframe(Sparksession):
      
      def __init__(self):
        Sparksession.__init__(self)
        log.info("Class Dataframe initialized")

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
           log.info(f"INPUT DATAFRAME COUNT FOR {src_path} : {dataframe.count()}")
        except Exception as e:
           log.warning(f"Issue while reading from path : {src_path}")
           log.error("IllegalArgumentException",exc_info=True)
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
            log.warning(f"Sheet name or Excel path is incorrect - {sheet_name} : {path}")
            log.error("IllegalArgumentException",exc_info=True)
            self.spark.stop()
            sys.exit()
         log.info(f"Loaded Excel sheet [{sheet_name}] as Dataframe")
         return dataframe
      
      def array_to_string(self,dataframe,col_name):
            dataframe=(dataframe.withColumn(col_name,col(col_name).cast("string"))
                .withColumn(col_name,regexp_replace(col_name, r'\[|\]|"','')))
            log.info(f"{col_name} parsed to string")
            return dataframe

      def flatten(self,df,array_info)->DataFrame:
         """
        flatten function recursively creates column for every object key /
          explode into new rows if it is an array.

        :param df: dataframe which you want to flatten
        :return  : Flattened Dataframe  
        """
         
         log.info(f"Flattening started for the ")
         array_cols = [info["array_col"] for info in array_info]
         complex_fields = dict([(field.name, field.dataType)
                                   for field in df.schema.fields
                                   if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])
         while len(complex_fields)!=0:
            col_name=list(complex_fields.keys())[0]
            log.debug(f"Processing :{col_name} Type : {str(type(complex_fields[col_name]))}")

            if (type(complex_fields[col_name]) == StructType):
               expanded = [col(col_name+'.'+k).alias(col_name+'_'+k) for k in [ n.name for n in  complex_fields[col_name]]]
               df=df.select("*", *expanded).drop(col_name)

            elif (type(complex_fields[col_name]) == ArrayType):
               log.info(f"ARRAY COLUMN : {col_name} ")
               if col_name in array_cols:
                        for info in array_info:
                                log.info(f"ARRAY INFO :{info}")
                                if col_name==info['array_col'] and "pivot_col" in info.keys() :
                                          df=self.pivot_array_column(df,info['array_col'],info['pivot_col'])
                                          break

                                elif col_name==info['array_col'] and "pivot_col" not in info.keys() :                     
                                         df=df.withColumn(col_name,explode_outer(col_name))
                                         break
                                else:
                                    log.info(complex_fields[col_name],info['array_col'])
                                    sys.exit(1)
                                    break
               else:
                    df=self.array_to_string(df,col_name)
                    

            complex_fields = dict([(field.name, field.dataType)
                                   for field in df.schema.fields
                                   if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])
         log.info(f"Flattening ended ")
         df.printSchema()
         return df
      
      
    

      def exp_outer(self,column:str,dataframe)-> DataFrame:
          dataframe=dataframe.withColumn(column,explode_outer(column))
          return dataframe
      
      def rename_duplicate_columns(self,dataframe):
            dataframe=dataframe.toDF(*[c.lower() for c in dataframe.columns])
            columns = dataframe.columns
            sql_columns=dataframe.columns
            duplicate_column_indices = list(set([col for col in columns if columns.count(col) > 1]))
            if len(duplicate_column_indices)>0:
                for name in duplicate_column_indices:
                    log.warning(f"Duplicate column {name} found")
                    column=f"COALESCE("
                    for i in range(columns.count(name)):
                        sql_columns.remove(name)
                        column=column+f"{name}_{i},"
                        index=columns.index(name)
                        columns[index]=f"{name}_{i}"
                    column=column[:-1]+ f') AS {name}'
                    sql_columns.append(column)
                dataframe.toDF(*columns).createOrReplaceTempView("stage")
                dataframe=self.spark.sql(f"""SELECT {','.join(sql_columns)} FROM stage""")
            return dataframe  
      

      def remove_duplicate_columns(self,dataframe):
            columns = dataframe.columns
            sql_columns=dataframe.columns
            duplicate_column_indices = list(set([col for col in columns if columns.count(col) > 1]))
            if len(duplicate_column_indices)>0:
                for name in duplicate_column_indices:
                    log.warning(f"Duplicate column {name} found")
                    index=columns.index(name)
                    columns[index]=f"{name}_1"
                    sql_columns.remove(name)

                dataframe.toDF(*columns).createOrReplaceTempView("stage")
                dataframe=self.spark.sql(f"""SELECT {','.join(sql_columns)} FROM stage""")
            
             
            return dataframe  

      def get_metadata(self,df:DataFrame,final_table:str,raw_table:str,database:str)->list:
            """
            get_metadata function filters column mapping (excel) with raw sources 
            and stores its fields & respective dest_column,data type ,transformation , PII flags.

            :param df         : Column mapping excel as dataframe
            :param final_table: Final Destination table
            :param raw_table  : Name of the source for Final Destination table
            :param database   : Source database name for the raw_table
            :return           : list of source column details
            """
            column_details=(df.filter((col("DEST_TABLE")=="customer") )
                            .selectExpr("lower(DEST_COLUMN)")
                            ).collect()
            duplicates = [item for item, count in Counter([i[0] for i in column_details ]).items() if count > 1]

          
            if len(duplicates) >0:
                    self.spark.stop()
                    log.error(F"DUPLICATE COLUMNS FOUND IN DESTINATION TABLE : {duplicates}",exc_info=True)
                    sys.exit()

            column_details=(df.filter((col("DEST_TABLE")==final_table) &
                                       (col("SRC_COLLECTION")==raw_table)  &
                                         (col("SRC_DB")==database))
                            .selectExpr("LOWER(REPLACE(SRC_FIELD, '.', '_'))","DEST_COLUMN","Transformation","PII","SRC_FIELD_DATATYPE")
                            ).collect()
            
            

            return column_details       


      def transform_cmap(self,dataframe)-> DataFrame:
           """
            transform_cmap function curates column mapping (excel) by spliting 
            and exploding as new rows for destination table column.
            EX: ROW1=['cust_profile','acc_transactions]
              -> ROW1=['cust_profile'] ,ROW2=['acc_transactions']

            :param dataframe  : Column mapping excel as dataframe
            :return           : Curated dataframe
            """
           dataframe=dataframe.withColumn('DEST_TABLE', split(dataframe['DEST_TABLE'], ','))
           log.info("Column_mapping excel metadata imported")
           return self.exp_outer("DEST_TABLE",dataframe)


      def convert_to_ist(self,dataframe,col_name):
          dataframe=dataframe.withColumn(col_name,col(col_name).cast("timestamp")) \
                    .selectExpr("*",f"{col_name} + INTERVAL 5 hours 30 minutes as {col_name}_1").drop(col_name) \
                    .withColumnRenamed(f"{col_name}_1",col_name)
          log.info(f"{col_name} converted to IST ")
          return dataframe

      
      def epoch_to_timestamp(self,dataframe,col_name):
           dataframe=dataframe.withColumn(col_name,dataframe[col_name].substr(1, 10).cast("long"))
           
           log.info(f"{col_name} epoch converted to timestamp ")
           dataframe=self.convert_to_ist(dataframe,col_name)
          
           return dataframe
           
      def str_to_ist(self,dataframe,col_name):
           dataframe = dataframe.withColumn(col_name, to_timestamp(col_name, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
           
           log.info(f"{col_name} string converted to timestamp ")
           dataframe=self.convert_to_ist(dataframe,col_name)
           
           return dataframe

      
      
      def dob_year_extract(self,dataframe,col_name):
           
           dataframe = dataframe.withColumn('age', (psf.months_between(current_date(), psf.col(col_name)) / 12).cast('int'))
           dataframe = dataframe.withColumn(col_name, year(col(col_name)).cast('string'))
           
           log.info(f"{col_name} extracted year data")
          
           return dataframe
      
      
      def rename_and_cast(self,dataframe,old_col_name,new_col_name,datatype):
          log.info(f"{old_col_name} renaming as - {new_col_name} & casting as {datatype} ")
          dataframe=dataframe.withColumnRenamed(old_col_name,new_col_name).withColumn(new_col_name,col(new_col_name).cast(datatype))
          return dataframe 
      
      def hash_pii(self,dataframe,col_name):
          dataframe=dataframe.withColumn(col_name, sha2(col(col_name), 256))
          log.warning(f"PII {col_name} hashed ")
          return dataframe

      def transformations(self,dataframe,column_metadata:list):
          """
            transformations function takes source column metadata ,if column exists
            it does the respective transformation else it is treated as missed column

            :param dataframe  : Dataframe After Flattening
            :param column_metadata  : list of field metadata for one source
                                      column_metadata[0] - source field name value
                                      column_metadata[1] - to be renamed field name value
                                      column_metadata[2] - transformation flag
                                      column_metadata[3] - PII flag
                                      column_metadata[4] - field datatype
            :return                 : Transformed dataframe ,missed columns list
            """
          it_count=0
          m_cols=[]
          df_cols=[i.lower() for i in dataframe.columns]
          for col_details in column_metadata:   
            if col_details[0] in df_cols:
                dataframe=self.rename_and_cast(dataframe,col_details[0],col_details[1],col_details[4])
                if col_details[2]=="udf_utc_to_ist" :
                    dataframe=self.convert_to_ist(dataframe,col_details[1])
                elif col_details[2]=="udf_epoch_to_ist" :
                    dataframe=self.epoch_to_timestamp(dataframe,col_details[1])
                    
                elif col_details[2]=="udf_str_to_ist" :
                    dataframe=self.str_to_ist(dataframe,col_details[1])
                    

                if col_details[2]=="extract_year" :
                      dataframe=self.dob_year_extract(dataframe,col_details[1])   
                
                if col_details[3]=='Y':
                    dataframe= self.hash_pii(dataframe,col_details[1])

                if it_count% 100 ==0:
                    self.checkpoint(dataframe)
                    
                it_count=it_count+1
            else:

                log.warning(f"{col_details[0]} not found")
                m_cols.append(col_details[1]) 
            
          return dataframe,m_cols
        
        
      def slt_col_rm_records(self,dataframe,listofcols,details,req_raw_unq):
          """
            slt_col_rm_records function runs for every source , selects only the columns
            mapped in the excel column mappings and removes old records in the batch for
            the given unique id.

            :param dataframe        : Dataframe After transformations
            :param listofcols       : All fields in c_map minus missed columns for that source
            :param unique_id        : field that is used to uniquely identify the rows
            :param incremental_key  : Timestamp field
            :return                 : Transformed dataframe ,missed columns list
            """
          dataframe.createOrReplaceTempView(details['name'])
          unique_id=details['unique_id']
          if details['unique_id'] in listofcols :
               log.warning(f"Unique id - {details['unique_id']} exists")

          else:
               listofcols.append(details['unq_sql'] + ' as ' +details['unique_id'])
               log.warning(f"Added Unique id - {details['unq_sql'] + ' as ' +details['unique_id']} ")
          sql_string=','.join(listofcols)  
      
          log.info(f"SQL STRING - {sql_string} ")
          dataframe=self.spark.sql(f"""SELECT {sql_string}
          FROM {details['name']} """).dropDuplicates()
          
          log.info(f"Incremental Key : {details['incremental_key']}")
          listofcols.remove(details['incremental_key'])
          w = psw.Window.partitionBy(unique_id)
          dataframe=(dataframe.withColumn("max_tmp", psf.max(details['incremental_key']).over(w))
                 .filter(psf.col("max_tmp") == psf.col(details['incremental_key']))).drop(psf.col("max_tmp"))
          log.info(f"Removed old records with partition on {unique_id} and incremental key {details['incremental_key']}")
          if req_raw_unq==False:
                   listofcols.remove(details['unique_id']) 
                   log.warning(f"Removed Unique id - {details['unique_id']} ")
          log.info(f"selected columns {listofcols}")
          return dataframe,listofcols
      
      def create_table(self,table_details:dict,shuffle)-> None:
            """
            create_table function runs for first load ,it overwrites data in the given path
            (if data exists) and creates delta table with mentioned partitioned
            column ,each partition folder containing only one file.

            :param table_details  : Details of Destination table
                                    table_details['partition_columns'] -partition column
                                    table_details['path']- S3 path 
            :return               : None
            """

            row_count=self.dataframe.count()
            log.info(f" Creating Table with Rows : {row_count}")
            if shuffle:
                self.dataframe.coalesce(1).write \
                        .partitionBy(table_details['partition_columns']) \
                        .option("compression", "snappy")  \
                        .mode("overwrite")  \
                        .format("delta")\
                        .save(table_details['path'])
            else:
                self.dataframe.write \
                        .partitionBy(table_details['partition_columns']) \
                        .option("compression", "snappy")  \
                        .mode("overwrite")  \
                        .format("delta")\
                        .save(table_details['path'])
            log.info("Table Created")

      def update_table(self,table_details:dict,DeltaTable)->None:
            """
            update_table function runs for incremental load ,it checks the final table
            If unique id exists and inc key of incremental load greater than inc key 
            of final table then it updates the record ,
            If unique does not exist , it creates new record in the final table 

            :param table_details  : Details of Destination table
                                    table_details['unique_id'] -unique_id
                                    table_details['incremental_key']- incremental_key
            :return               : None
            """
            log.info("*********update_table Started************")
            
            log.info(f"Incremental Load Volume : {self.dataframe.count()} rows")
            deltaTable_full = DeltaTable.forPath(self.spark, table_details['path'])
            log.info(f"Full Load Volume : {deltaTable_full.toDF().count()} rows")
            deltaTable_full.alias('fl') \
                        .merge(
                            self.dataframe.alias('dl'),
                            f"fl.{table_details['unique_id']} = dl.{table_details['unique_id']} "  ) \
                .whenMatchedUpdateAll(condition=f" dl.{table_details['incremental_key']}> fl.{table_details['incremental_key']}") \
                .whenNotMatchedInsertAll() \
                .execute()
            log.info("Table updated")

      def update_table_no_inc_key(self,table_details:dict,DeltaTable):
            """
            update_table_no_inc_key function runs for incremental load ,it checks the final table
            If unique id exists then it updates the record ,
            If unique does not exist , it creates new record in the final table 

            :param table_details  : Details of Destination table
                                    table_details['unique_id'] -unique_id
            :return               : None
            """
            log.info("*********update_table_no_inc_key Started************")
            log.info(f"Incremental Load Volume : {self.dataframe.count()} rows")
            deltaTable_full = DeltaTable.forPath(self.spark, table_details['path'])
            log.info(f"Full Load Volume : {deltaTable_full.toDF().count()} rows")
            deltaTable_full.alias('fl') \
                        .merge(
                            self.dataframe.alias('dl'),
                            f"fl.{table_details['unique_id']} = dl.{table_details['unique_id']} "  ) \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
            log.info("Table updated")
      def append_table(self,table_details:dict,DeltaTable):
            """
            update_table_no_inc_key function runs for incremental load ,it checks the final table
            If unique id exists then it updates the record ,
            If unique does not exist , it creates new record in the final table 

            :param table_details  : Details of Destination table
                                    table_details['unique_id'] -unique_id
            :return               : None
            """
            log.info("*********Appending Started************")
            log.info(f"Incremental Load Volume : {self.dataframe.count()} rows")
            deltaTable_full = DeltaTable.forPath(self.spark, table_details['path'])
            log.info(f"Full Load Volume : {deltaTable_full.toDF().count()} rows")
            deltaTable_full.alias('fl') \
                        .merge(
                            self.dataframe.alias('dl'),
                            f"fl.{table_details['unique_id']} = dl.{table_details['unique_id']} ") \
                .whenNotMatchedInsertAll() \
                .execute()
            log.info("Table updated")

      def get_delta_metrics(self,table_details:dict):
            """
            get_delta_metrics function runs after table creation/updation and 
            returns row_count- final table count
                    num_files- number of s3 files exists for the table
                    size     - Total storage in bytes
                    version  - version of the table
                    
            :param table_details  : Details of Destination table
                                    table_details['path'] -path of final delta table
            :return               : Delta table metrics
            """
            
            deltaTable_full = self.DeltaTable.forPath(self.spark, table_details['path'])
            details=deltaTable_full.detail().collect()[0]
            row_count=deltaTable_full.toDF().count()
            num_files=details['numFiles']
            size=details['sizeInBytes']
            version=deltaTable_full.history().selectExpr("max(version) as version").dropDuplicates().collect()[0][0]

            return row_count,num_files,size,version
      
      def spark_read_cdf(self,path:str,version:int,first_load:bool):
            """
            spark_read_cdf function reads curated delta tables ,
            Full load if it is first load or version of delta table = 0
            Incremental load- Reads only the changed data b/w startingVersion and endingVersion
                    
            :param path       : S3 path of Destination table
            :param version    : Latest version of delta table
            :param first_load : first load / incremental load
            :return           : Curated table dataframe
            """
            if first_load==True or version==0:
                    dataframe=(self.spark.read
                    .format("delta")
                    .load(path))
            else:
                    dataframe=(self.spark.read
                    .format("delta")
                    .option("readChangeFeed", "true")
                    .option("startingVersion", version-1)
                    .option("endingVersion",version) 
                    .load(path) 
                    .filter("_change_type != 'update_preimage'"))
            log.info("Read Dataframe")
            return dataframe
      
      def filter(self,dataframe,column_name,value):
            if value=="null":
                 dataframe=dataframe.filter(col(column_name).isNull()==False)
            else:
                dataframe=dataframe.filter(lower(col(column_name))==value)
            log.warning(f"Filtered {column_name} with {value}")
            return dataframe
      def checkpoint(self,dataframe):
           self.spark.sparkContext.setCheckpointDir("s3a://xxxx-de-beta-data/emr/checkpoints/")
           dataframe.checkpoint()
           dataframe.count()   
           log.info("Dataframe Checkpointed")

      def start_crawler_run(self,crawler_name):
             log.info(f"Started Glue Crawler :{crawler_name}")
             glue_client = boto3.client('glue', region_name='ap-south-1')

             response = glue_client.start_crawler(Name=crawler_name)
             log.info(response)

      def pivot_array_column(self, df, array_col, pivot_col):
            
            exploded_df = df.select("*", explode_outer(array_col).alias("assoc")).drop(array_col)
            
            main_cols=exploded_df.columns
            main_cols.remove("assoc")
            
            
            unique_categories = exploded_df.select(f"assoc.{pivot_col}").distinct().rdd.flatMap(lambda x: x).collect()
            log.info(f"All UNique Categories - {unique_categories}")
            
            all_cols=exploded_df.select("assoc.*").columns
            all_cols.remove(pivot_col)
            
            pivot_exprs = []

            for cols in all_cols:
                pivot_expr = expr(f"first(assoc.{cols}) as {cols}")
                pivot_exprs.append(pivot_expr)
                
            df = exploded_df.groupBy(*main_cols).pivot(f"assoc.{pivot_col}").agg(*pivot_exprs)
            
            log.info(f"PIVOTED AND AGGREGATED DATAFRAME WITH {array_col}.{pivot_col}")
            
            columns_to_keep = [col for col in df.columns if not col.startswith("null_")]

            filtered_df = df.select(columns_to_keep)
            
            return filtered_df.dropDuplicates()




class Main(Dataframe,Queryconfig):
    def __init__(self,final_table,table_metadata,DeltaTable,filters=None):
         Queryconfig.__init__(self)
         Dataframe.__init__(self)
         self.start_time=super().get_start_time()
         self.final_table=final_table
         self.database="configs"
         self.__excel_config_path='s3://xxxx-de-beta-data/de-beta-data-lake/data-lake/configs/source-to-target-mappings/source_to_target_mapping.xlsx'
         self.c_map=None
         self.DeltaTable=DeltaTable
         self.dataframe=None
         self.tbl_details=table_metadata
         self.filters=filters
         self.all_fields=[]
         self.multi_source=None
         self.array_info=[]
         self.req_raw_unq=None
         log.info("Class Main initialized")
         log.info(f"Final Table : {self.final_table}")

    def set_mappings(self)->None:
         """
        set_mappings function runs only when the given table metadata is stored in excel,
        reads the column mappings and set them
        as attributes for the Class obect.
                
        :return           : None
        """
         self.c_map=super().transform_cmap(super().read_excel("column_mapping",self.__excel_config_path))
     
    def check_data_exits(self):
         if self.dataframe.rdd.isEmpty():
              self.spark.stop()
              log.warning("No incremental data in final dataframe")
         else:
              pass
    def multithread_sources(self,source):
            db=source[0]
            table=source[1]
            config=source[2]
            query=f"SELECT * from {self.database}.{config} where name= '{table}' and database= '{db}'"
            details=super().get_config_metadata(query,config)

                
            dataframe=super().read(details['src_path'],self.tbl_details['last_modified'],details['format'],details['source'])
        
            dataframe=super().rename_duplicate_columns(dataframe)
        
            dataframe=super().flatten(dataframe,self.array_info)
                

            if self.filters != None:
                    src_details=self.filters[table]
                    for i in range(len(src_details)):
                        dataframe=super().filter(dataframe,src_details[i]['column'],src_details[i]['value'])
                        try:
                            details['unique_id']=src_details[i]['unique_id']
                            details['incremental_key']=src_details[i]['incremental_key']
                            log.warning(f"Override source details for {table}")
                        except:
                             pass


            column_metadata=super().get_metadata(self.c_map,self.final_table,table,db)


            dataframe,m_cols=super().transformations(dataframe,column_metadata)
            log.info(f"Missed Columns in {table} - {m_cols}")
            f_cols=list((Counter([x[1] for x in column_metadata]) - Counter(m_cols)).elements()) 
            dataframe,f_cols=super().slt_col_rm_records(dataframe, f_cols,details,self.req_raw_unq)
            self.all_fields=self.all_fields+f_cols
            log.info(f"{table} count - {dataframe.count()}")
            dataframe.cache()
            dataframe.createOrReplaceTempView(table)
                 


    def get_source_dataframes(self,req_raw_unq=False,array_info=[])->None:
        """
        get_source_dataframes function does the following actions:
        -store curated table details : path,unique_id,incremental_key,partition_columns,
                                        last_modified,version,crawler,first_load
        -get all sources that are mapped to final table
        -for every raw source:
            - gather its metadta from raw config : src_path,unique_id,incremental_key,format
            -read
            -flatten
            -get_metadata
            -transformations
            -slt_col_rm_records
            -creates a temporary view of final dataframe for that raw source
        -setting all_fields stores all the fields from column mappings that are present in sources
                
        :return     : None
        """
        
        sources=self.c_map.filter(col("DEST_TABLE")==self.final_table).select("SRC_DB","SRC_COLLECTION","SRC_CONFIG").dropDuplicates().collect()
        log.info(f" Sources for {self.final_table} : {sources}")
        self.array_info=array_info
        self.req_raw_unq=req_raw_unq
        self.multi_source=True if len(sources)>1 else False
        pool=ThreadPool(len(sources))
        pool.map(self.multithread_sources,sources)
            
            
    def create_update_table(self,shuffle=True)->None:
          """
            create_update_table function does the following actions:
            -genertaes the final sql to create final dataframe 
            -create_table / update_table based on first load
            -update metadata in configs tables
            -run the final table crawler
                
            :return     : None
            """
          self.all_fields.remove(self.tbl_details['partition_columns']) if self.tbl_details['partition_columns'] in self.all_fields else None
          sql_string= f"""SELECT {self.tbl_details['id_sql']}  AS {self.tbl_details['unique_id']},{",".join(self.all_fields)} , {self.tbl_details['incremental_sql']} AS
          {self.tbl_details['incremental_key']} , {self.tbl_details['partition_sql']} AS {self.tbl_details['partition_columns']} FROM {self.tbl_details['from_sql']}""" 
          
          log.info(f"Generated SQL :  {sql_string} ")

          self.dataframe=self.spark.sql(sql_string).dropDuplicates()

          self.check_data_exits()
          self.dataframe=super().remove_duplicate_columns(self.dataframe)

          
          if self.tbl_details['first_load']==True:
             super().create_table(self.tbl_details,shuffle)
          else:
                if self.multi_source:
                        self.all_fields.append(self.tbl_details['partition_columns'])
                        self.dataframe.createOrReplaceTempView("delta")
                        self.DeltaTable.forPath(self.spark, self.tbl_details['path']).toDF().createOrReplaceTempView("full")
                        sql_string=f""" SELECT d.{self.tbl_details['unique_id']},d.{self.tbl_details['incremental_key']},{",".join([f"coalesce(d.{i},f.{i}) as {i}" for i in self.all_fields])}
                                        FROM delta d
                                        LEFT JOIN full f
                                        ON d.{self.tbl_details['unique_id']}=f.{self.tbl_details['unique_id']}"""
                        log.info(f"MULTI SOURCE QUERY : {sql_string}")
                        log.info(f"Setted multi_source {self.multi_source}")
                        self.dataframe=self.spark.sql(sql_string)
                super().update_table(self.tbl_details,self.DeltaTable) if self.tbl_details['mode']=='upsert' else  super().append_table(self.tbl_details,self.DeltaTable)
                
              
              
          row_count,num_files,size,version=super().get_delta_metrics(self.tbl_details)
          query=f"""UPDATE  configs.curated
            SET last_modified ='{self.start_time}',
          row_count={row_count},
          num_files={num_files},
          size_in_bytes={size},
          version={version}
          {self.tbl_details['created_at']}
          where name= '{self.final_table}' """
          response=super().run_athena_query(query)



    def get_curated_data_frames(self)->None:
        """
        get_curated_data_frames function is used in creating Aalytics table.
        -store analytics final table metadata
        -for each curated source table:
             -read full / incremental load and create as temp view
        -generate data from analytics table query value
        -set the the final dataframe 

        :return : None
        """
        
        for i in self.tbl_details['sources'].split(","):
            query=f"SELECT * from configs.curated where name= '{i}' "
            details=super().get_config_metadata(query,"curated")

            dataframe=super().spark_read_cdf(details['path'],int(details['version']),self.tbl_details['first_load'])
            log.info(f"{i} loaded as dataframe with total count : {dataframe.count()}")
            dataframe.createOrReplaceTempView(i)
    

    def set_dataframe(self,dataframe):
         self.dataframe=dataframe



    def write_staging(self):
         partitions=20 if self.tbl_details['first_load']==True else 1
         log.info(f"Partition set : {partitions}")
         dataframe=super().read(self.tbl_details['src_path'],self.tbl_details['last_modified'],self.tbl_details['format'],"kafka")
         dataframe=super().rename_duplicate_columns(dataframe)
         dataframe=dataframe.repartition(partitions)
         (dataframe.write.mode("append")
           .option("maxRecordsPerFile", 1000000)
          .option("compression", "gzip")
          .format("json")
          .save(f"{self.tbl_details['final_path']}/{now}/"))
          
         log.info(f"TOTAL COUNT-{dataframe.count()}")
         
         log.info(F"{self.tbl_details['name']} appended in staging  ")
         query=f"""UPDATE  configs.staging
            SET last_modified ='{self.start_time}'
            where name= '{self.final_table}' """
         response=super().run_athena_query(query)
         log.info("Job Succesfully completed")

    def compact_delta(self,path,zorder=None):
         deltaTable =  self.DeltaTable.forPath(self.spark, path) 

         deltaTable.optimize().executeCompaction() if zorder ==None else deltaTable.optimize().executeZOrderBy(zorder)
         log.info("Compaction Completed")
        

    def vaccum_delta(self,path,hours):
         deltaTable =  self.DeltaTable.forPath(self.spark, path) 
         deltaTable.vacuum(0.01) 
         log.info(f"Vacuum for table path {path} done for hours: {hours}")
         row_count,num_files,size,version=super().get_delta_metrics(self.tbl_details)
         query=f"""UPDATE  configs.curated
            SET last_optimised =CURRENT_TIMESTAMP,
          num_files={num_files},
          size_in_bytes={size},
          version={version}
          where name= '{self.final_table}' """
         response=super().run_athena_query(query)
         super().start_crawler_run(self.tbl_details['crawler'])
         log.info("Job Succesfully completed")

    def get_dataframe(self):
         return self.dataframe
        
         
