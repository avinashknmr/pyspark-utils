import sys
import logging
import boto3
import re
import json
import base64

from collections import Counter
from datetime import datetime, timezone
from multiprocessing.pool import ThreadPool

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.window as psw
import pyspark.sql.functions as psf

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)-8s | %(name)-6s | %(message)s', datefmt='%Y-%m-%d %I:%M:%S %p')
logger = logging.getLogger("ETL")

def get_input_arguments():
    ia={}
    if len(sys.argv)>1:
        for key,value in eval(sys.argv[1]).items():
            ia[key]=value
            logger.info(f"Input Arguments passed - {key} : {value}")
        
    else:
        logger.info("Input Arguments passed")
    return ia

class Queryconfig:
    def __init__(self):
        self.database="configs"
        self.raw_config="raw"
        self.curated_config="curated"
        self.bi_config="analytics"
        self.bucket= "niyo-de-prod-data"
        self.path= "athena_query_output"
        self.workgroup="ETL"
        self.client=boto3.client('athena', region_name='ap-south-1')
        logger.info("Class Queryconfig initialized")
        
    def run_athena_query(self,query:str)-> dict:

        """
        run_athena_query function runs a SQL query on Athena .

        :param query: sql query
        :return     : Output of SQL query 
        """
        retry=True
        logger.info(f"Query: {query}")
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
        
        logger.info(f"Query status : {state}")
        if state=='FAILED' and retry==False:
            logger.warning("Query failed - Please check the Config table ")
            logger.error("IllegalArgumentException",exc_info=True)
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
            logger.info(f"{response['ResultSet']['Rows'][0]['Data'][val]['VarCharValue']}: {response['ResultSet']['Rows'][1]['Data'][val]['VarCharValue']}")
        if config_table!="raw":
            details['first_load']=True if details['last_modified']=='2016-01-01T00:00:00' else False
            logger.info(f"First Load : {details['first_load']}")
            details['created_at']=',created_at=current_timestamp' if details['last_modified']=='2016-01-01T00:00:00' else ''
        return details



           


