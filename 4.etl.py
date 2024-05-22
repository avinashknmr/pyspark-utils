class Main(Dataframe,Queryconfig):
    def __init__(self,final_table,table_metadata,DeltaTable,filters=None):
        Queryconfig.__init__(self)
        Dataframe.__init__(self)
        self.start_time=super().get_start_time()
        self.final_table=final_table
        self.database="configs"
        self.__excel_config_path='s3://niyo-de-prod-data/de-prod-data-lake/data-lake/configs/source-to-target-mappings/source_to_target_mapping.xlsx'
        self.c_map=None
        self.DeltaTable=DeltaTable
        self.dataframe=None
        self.tbl_details=table_metadata
        self.filters=filters
        self.all_fields=[]
        self.multi_source=None
        self.array_info=[]
        self.req_raw_unq=None
        logger.info("Class Main initialized")
        logger.info(f"Final Table : {self.final_table}")

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
            logger.warning("No incremental data in final dataframe")
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
                    logger.warning(f"Override source details for {table}")
                except:
                        pass


        column_metadata=super().get_metadata(self.c_map,self.final_table,table,db)


        dataframe,m_cols=super().transformations(dataframe,column_metadata)
        logger.info(f"Missed Columns in {table} - {m_cols}")
        f_cols=list((Counter([x[1] for x in column_metadata]) - Counter(m_cols)).elements()) 
        dataframe,f_cols=super().slt_col_rm_records(dataframe, f_cols,details,self.req_raw_unq)
        self.all_fields=self.all_fields+f_cols
        logger.info(f"{table} count - {dataframe.count()}")
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
        logger.info(f" Sources for {self.final_table} : {sources}")
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
        
        logger.info(f"Generated SQL :  {sql_string} ")

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
                logger.info(f"MULTI SOURCE QUERY : {sql_string}")
                logger.info(f"Setted multi_source {self.multi_source}")
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
            logger.info(f"{i} loaded as dataframe with total count : {dataframe.count()}")
            dataframe.createOrReplaceTempView(i)
    
    def set_dataframe(self,dataframe):
        self.dataframe=dataframe

    def write_staging(self):
        now = datetime.now().strftime("%Y-%m-%d")
        partitions=20 if self.tbl_details['first_load']==True else 1
        logger.info(f"Partition set : {partitions}")
        dataframe=super().read(self.tbl_details['src_path'],self.tbl_details['last_modified'],self.tbl_details['format'],"kafka")
        dataframe=super().rename_duplicate_columns(dataframe)
        dataframe=dataframe.repartition(partitions)
        (dataframe.write.mode("append")
        .option("maxRecordsPerFile", 1000000)
        .option("compression", "gzip")
        .format("json")
        .save(f"{self.tbl_details['final_path']}/{now}/"))
        
        logger.info(f"TOTAL COUNT-{dataframe.count()}")
        
        logger.info(F"{self.tbl_details['name']} appended in staging  ")
        query=f"""UPDATE  configs.staging
        SET last_modified ='{self.start_time}'
        where name= '{self.final_table}' """
        response=super().run_athena_query(query)
        logger.info("Job Succesfully completed")

    def compact_delta(self,path,zorder=None):
        deltaTable =  self.DeltaTable.forPath(self.spark, path) 

        deltaTable.optimize().executeCompaction() if zorder ==None else deltaTable.optimize().executeZOrderBy(zorder)
        logger.info("Compaction Completed")
        

    def vaccum_delta(self,path,hours):
        deltaTable =  self.DeltaTable.forPath(self.spark, path) 
        deltaTable.vacuum(0.01) 
        logger.info(f"Vacuum for table path {path} done for hours: {hours}")
        row_count,num_files,size,version=super().get_delta_metrics(self.tbl_details)
        query=f"""UPDATE  configs.curated
        SET last_optimised =CURRENT_TIMESTAMP,
        num_files={num_files},
        size_in_bytes={size},
        version={version}
        where name= '{self.final_table}' """
        response=super().run_athena_query(query)
        super().start_crawler_run(self.tbl_details['crawler'])
        logger.info("Job Succesfully completed")

    def get_dataframe(self):
        return self.dataframe