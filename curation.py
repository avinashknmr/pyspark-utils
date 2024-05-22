import logging
from datetime import datetime, timezone
import boto3
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)-8s | %(name)-6s | %(message)s', datefmt='%Y-%m-%d %I:%M:%S %p')
logger = logging.getLogger("CURATE")

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
      
    def array_to_string(self,dataframe,col_name):
        dataframe=(dataframe.withColumn(col_name,col(col_name).cast("string"))
            .withColumn(col_name,regexp_replace(col_name, r'\[|\]|"','')))
        logger.info(f"{col_name} parsed to string")
        return dataframe

    def flatten(self,df,array_info)->DataFrame:
         """
        flatten function recursively creates column for every object key /
          explode into new rows if it is an array.

        :param df: dataframe which you want to flatten
        :return  : Flattened Dataframe  
        """
         
         logger.info(f"Flattening started for the ")
         array_cols = [info["array_col"] for info in array_info]
         complex_fields = dict([(field.name, field.dataType)
                                   for field in df.schema.fields
                                   if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])
         while len(complex_fields)!=0:
            col_name=list(complex_fields.keys())[0]
            logger.debug(f"Processing :{col_name} Type : {str(type(complex_fields[col_name]))}")

            if (type(complex_fields[col_name]) == StructType):
               expanded = [col(col_name+'.'+k).alias(col_name+'_'+k) for k in [ n.name for n in  complex_fields[col_name]]]
               df=df.select("*", *expanded).drop(col_name)

            elif (type(complex_fields[col_name]) == ArrayType):
               logger.info(f"ARRAY COLUMN : {col_name} ")
               if col_name in array_cols:
                        for info in array_info:
                                logger.info(f"ARRAY INFO :{info}")
                                if col_name==info['array_col'] and "pivot_col" in info.keys() :
                                          df=self.pivot_array_column(df,info['array_col'],info['pivot_col'])
                                          break

                                elif col_name==info['array_col'] and "pivot_col" not in info.keys() :                     
                                         df=df.withColumn(col_name,explode_outer(col_name))
                                         break
                                else:
                                    logger.info(complex_fields[col_name],info['array_col'])
                                    sys.exit(1)
                                    break
               else:
                    df=self.array_to_string(df,col_name)
                    

            complex_fields = dict([(field.name, field.dataType)
                                   for field in df.schema.fields
                                   if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])
         logger.info(f"Flattening ended ")
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
                logger.warning(f"Duplicate column {name} found")
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
                logger.warning(f"Duplicate column {name} found")
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
                logger.error(F"DUPLICATE COLUMNS FOUND IN DESTINATION TABLE : {duplicates}",exc_info=True)
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
        logger.info("Column_mapping excel metadata imported")
        return self.exp_outer("DEST_TABLE",dataframe)


    def convert_to_ist(self,dataframe,col_name):
        dataframe=dataframe.withColumn(col_name,col(col_name).cast("timestamp")) \
                .selectExpr("*",f"{col_name} + INTERVAL 5 hours 30 minutes as {col_name}_1").drop(col_name) \
                .withColumnRenamed(f"{col_name}_1",col_name)
        logger.info(f"{col_name} converted to IST ")
        return dataframe

      
    def epoch_to_timestamp(self,dataframe,col_name):
        dataframe=dataframe.withColumn(col_name,dataframe[col_name].substr(1, 10).cast("long"))
        
        logger.info(f"{col_name} epoch converted to timestamp ")
        dataframe=self.convert_to_ist(dataframe,col_name)
        
        return dataframe
           
    def str_to_ist(self,dataframe,col_name):
        dataframe = dataframe.withColumn(col_name, to_timestamp(col_name, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
        
        logger.info(f"{col_name} string converted to timestamp ")
        dataframe=self.convert_to_ist(dataframe,col_name)
        
        return dataframe

      
      
    def dob_year_extract(self,dataframe,col_name):
        
        dataframe = dataframe.withColumn('age', (psf.months_between(current_date(), psf.col(col_name)) / 12).cast('int'))
        dataframe = dataframe.withColumn(col_name, year(col(col_name)).cast('string'))
        
        logger.info(f"{col_name} extracted year data")
        
        return dataframe
      
      
    def rename_and_cast(self,dataframe,old_col_name,new_col_name,datatype):
        logger.info(f"{old_col_name} renaming as - {new_col_name} & casting as {datatype} ")
        dataframe=dataframe.withColumnRenamed(old_col_name,new_col_name).withColumn(new_col_name,col(new_col_name).cast(datatype))
        return dataframe 
      
    def hash_pii(self,dataframe,col_name):
        dataframe=dataframe.withColumn(col_name, sha2(col(col_name), 256))
        logger.warning(f"PII {col_name} hashed ")
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

                logger.warning(f"{col_details[0]} not found")
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
            logger.warning(f"Unique id - {details['unique_id']} exists")

        else:
            listofcols.append(details['unq_sql'] + ' as ' +details['unique_id'])
            logger.warning(f"Added Unique id - {details['unq_sql'] + ' as ' +details['unique_id']} ")
        sql_string=','.join(listofcols)  
    
        logger.info(f"SQL STRING - {sql_string} ")
        dataframe=self.spark.sql(f"""SELECT {sql_string}
        FROM {details['name']} """).dropDuplicates()
        
        logger.info(f"Incremental Key : {details['incremental_key']}")
        listofcols.remove(details['incremental_key'])
        w = psw.Window.partitionBy(unique_id)
        dataframe=(dataframe.withColumn("max_tmp", psf.max(details['incremental_key']).over(w))
                .filter(psf.col("max_tmp") == psf.col(details['incremental_key']))).drop(psf.col("max_tmp"))
        logger.info(f"Removed old records with partition on {unique_id} and incremental key {details['incremental_key']}")
        if req_raw_unq==False:
                listofcols.remove(details['unique_id']) 
                logger.warning(f"Removed Unique id - {details['unique_id']} ")
        logger.info(f"selected columns {listofcols}")
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
        logger.info(f" Creating Table with Rows : {row_count}")
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
        logger.info("Table Created")

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
        logger.info("*********update_table Started************")
        
        logger.info(f"Incremental Load Volume : {self.dataframe.count()} rows")
        deltaTable_full = DeltaTable.forPath(self.spark, table_details['path'])
        logger.info(f"Full Load Volume : {deltaTable_full.toDF().count()} rows")
        deltaTable_full.alias('fl') \
                    .merge(
                        self.dataframe.alias('dl'),
                        f"fl.{table_details['unique_id']} = dl.{table_details['unique_id']} "  ) \
            .whenMatchedUpdateAll(condition=f" dl.{table_details['incremental_key']}> fl.{table_details['incremental_key']}") \
            .whenNotMatchedInsertAll() \
            .execute()
        logger.info("Table updated")

    def update_table_no_inc_key(self,table_details:dict,DeltaTable):
        """
        update_table_no_inc_key function runs for incremental load ,it checks the final table
        If unique id exists then it updates the record ,
        If unique does not exist , it creates new record in the final table 

        :param table_details  : Details of Destination table
                                table_details['unique_id'] -unique_id
        :return               : None
        """
        logger.info("*********update_table_no_inc_key Started************")
        logger.info(f"Incremental Load Volume : {self.dataframe.count()} rows")
        deltaTable_full = DeltaTable.forPath(self.spark, table_details['path'])
        logger.info(f"Full Load Volume : {deltaTable_full.toDF().count()} rows")
        deltaTable_full.alias('fl') \
                    .merge(
                        self.dataframe.alias('dl'),
                        f"fl.{table_details['unique_id']} = dl.{table_details['unique_id']} "  ) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
        logger.info("Table updated")
    def append_table(self,table_details:dict,DeltaTable):
        """
        update_table_no_inc_key function runs for incremental load ,it checks the final table
        If unique id exists then it updates the record ,
        If unique does not exist , it creates new record in the final table 

        :param table_details  : Details of Destination table
                                table_details['unique_id'] -unique_id
        :return               : None
        """
        logger.info("*********Appending Started************")
        logger.info(f"Incremental Load Volume : {self.dataframe.count()} rows")
        deltaTable_full = DeltaTable.forPath(self.spark, table_details['path'])
        logger.info(f"Full Load Volume : {deltaTable_full.toDF().count()} rows")
        deltaTable_full.alias('fl') \
                    .merge(
                        self.dataframe.alias('dl'),
                        f"fl.{table_details['unique_id']} = dl.{table_details['unique_id']} ") \
            .whenNotMatchedInsertAll() \
            .execute()
        logger.info("Table updated")

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
        logger.info("Read Dataframe")
        return dataframe
    
    def filter(self,dataframe,column_name,value):
        if value=="null":
                dataframe=dataframe.filter(col(column_name).isNull()==False)
        else:
            dataframe=dataframe.filter(lower(col(column_name))==value)
        logger.warning(f"Filtered {column_name} with {value}")
        return dataframe
    
    def checkpoint(self,dataframe):
        self.spark.sparkContext.setCheckpointDir("s3a://niyo-de-prod-data/emr/checkpoints/")
        dataframe.checkpoint()
        dataframe.count()   
        logger.info("Dataframe Checkpointed")

    def start_crawler_run(self,crawler_name):
        logger.info(f"Started Glue Crawler :{crawler_name}")
        glue_client = boto3.client('glue', region_name='ap-south-1')

        response = glue_client.start_crawler(Name=crawler_name)
        logger.info(response)

    def pivot_array_column(self, df, array_col, pivot_col):
        
        exploded_df = df.select("*", explode_outer(array_col).alias("assoc")).drop(array_col)
        
        main_cols=exploded_df.columns
        main_cols.remove("assoc")
        
        
        unique_categories = exploded_df.select(f"assoc.{pivot_col}").distinct().rdd.flatMap(lambda x: x).collect()
        logger.info(f"All UNique Categories - {unique_categories}")
        
        all_cols=exploded_df.select("assoc.*").columns
        all_cols.remove(pivot_col)
        
        pivot_exprs = []

        for cols in all_cols:
            pivot_expr = expr(f"first(assoc.{cols}) as {cols}")
            pivot_exprs.append(pivot_expr)
            
        df = exploded_df.groupBy(*main_cols).pivot(f"assoc.{pivot_col}").agg(*pivot_exprs)
        
        logger.info(f"PIVOTED AND AGGREGATED DATAFRAME WITH {array_col}.{pivot_col}")
        
        columns_to_keep = [col for col in df.columns if not col.startswith("null_")]

        filtered_df = df.select(columns_to_keep)
        
        return filtered_df.dropDuplicates()