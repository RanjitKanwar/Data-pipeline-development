from datapipeline import config
from datapipeline import services
from datapipeline._properties import execution_step_property
from typeguard import typechecked
from pyspark.sql.functions import lit,to_timestamp,col
from  datetime import datetime
from typing import Any,Optional,Dict,List,Tuple
from datapipeline._properties import snowflake_property
from datapipeline.database import _mysql as mysql
from datapipeline.database import _snowflake as snowflake
from datapipeline.exceptions import *
from datapipeline._utils import  sanitize_name
import awswrangler as wr
from snowflake.connector.pandas_tools import write_pandas
import boto3
from multiprocessing.pool import ThreadPool
import logging
import pandas as pd
logger: logging.Logger = logging.getLogger(__name__)

@typechecked
def create_aws_glue_table(glue_database:str,
                          glue_table:str,
                          path:str,
                          source_file_format:str,
                          datalake_path:str,
                          rejected_records_file_path :str,
                          glue_file_format:Optional[str]=None,
                          mode :Optional[str]=None,
                          compression:Optional[str]=None,
                          partition_cols:Optional[List[str]]=None,
                          partition_base_column:Optional[str]=None,
                          process_execution_id:Optional[int]=-1,
                          boto3_session:Optional[boto3.Session]=None,
                          data_flatten:Optional[bool]=False) -> Dict[str,Any] :
    response={}
    step_list=[]
    execution_status=True
    step_create_aws_glue_table=execution_step_property(entity=glue_table,step="raw_data_to_datalake",start_dt=datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f'))
    is_all_corrupt_record=False
    try:
        boto3_session = boto3_session if boto3_session else services.botoServices.boto3_session()

        glue_partition_cols= partition_cols if  partition_cols else config.get_config('S3','PARTITION_KEYS').split(",")
        partition_base_col=partition_base_column
        sc=services.sparkServices.get_spark_session()
        df = services.sparkServices.load_data(path=path,spark_session=sc,format=source_file_format)
        logger.info(f' {glue_table} Initial dataframe')
        df.printSchema()
        df=df.withColumn("process_execution_id",lit(process_execution_id))
        columns_records_counts=services.sparkServices.dataframe_records_count_column_wise(df)
        step_create_aws_glue_table.records_cnt=columns_records_counts['process_execution_id']

        if '_corrupt_record' in columns_records_counts.keys():
            services.sparkServices.write_file(df=df.select("_corrupt_record"),path=rejected_records_file_path,format='json')
            df=df.drop('_corrupt_record')
            df=df.na.drop("all")
            step_create_aws_glue_table.reject_records_cnt=columns_records_counts['_corrupt_record']

            if  columns_records_counts['process_execution_id']==columns_records_counts['_corrupt_record'] :
                wr.s3.delete_objects(path,boto3_session=boto3_session)
                is_all_corrupt_record=True
                raise InvalidFile(f' {glue_table} all records are currputed')


        df = df.withColumn(partition_base_col, to_timestamp(col(partition_base_col)))
        df = services.sparkServices.drive_year_month_day_hour_columns(df=df,column=partition_base_col)

        df.cache()
        logger.info(f' {glue_table} modified dataframe with partition columns')
        df.printSchema()

        null_columns=  [key  for (key, value) in columns_records_counts.items() if value == '0']
        if null_columns:
            print(null_columns)
            df=df.drop(*null_columns)
            df.printSchema()

        fun_response=services.glueServices.dataframe_to_datalake(df,database=glue_database,table=glue_table,mode=mode,boto3_session=boto3_session,datalake_path=datalake_path,partition_cols=glue_partition_cols,compression=compression,format=glue_file_format)
        step_list.extend(fun_response)
        step_create_aws_glue_table.end_dt=datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
        step_create_aws_glue_table.status='Success'
        step_list.append(step_create_aws_glue_table.asDict())

    except Exception as e:
        logger.exception(e)
        execution_status=False
        step_create_aws_glue_table.description=str(e)
        step_create_aws_glue_table.status='Failed'
        step_create_aws_glue_table.end_dt=datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
        step_list.append(step_create_aws_glue_table.asDict())
        raise RuntimeError("Error Occured during etl job execution")
    finally:
        response['execution_status']=execution_status
        response['steps']=step_list
        response['is_all_corrupt_record']=is_all_corrupt_record
        return response


def refreshDatalaketable(glue_database:str,
                         glue_table:str,
                         path:str,
                         source_file_format:str,
                         datalake_path:str,
                         process_execution_id:int,
                         job_execution_id:int,
                         rejected_records_file_path:str,
                         boto3_session:Optional[boto3.Session]=None,
                         glue_file_format:Optional[str]=None,
                         mode :Optional[str]=None,
                         compression:Optional[str]=None,
                         partition_cols:Optional[List[str]]=None,
                         partition_base_column:Optional[str]=None
                         ):
    refreshDatalaketable_status=True
    try :
        response=create_aws_glue_table(glue_database=glue_database,
                                                          glue_table=glue_table,
                                                          path=path,
                                                          source_file_format=source_file_format,
                                                          datalake_path=datalake_path,
                                                          rejected_records_file_path =rejected_records_file_path,
                                                          glue_file_format=glue_file_format,
                                                          mode=mode,
                                                          compression=compression,
                                                          partition_cols=partition_cols,
                                                          partition_base_column=partition_base_column,
                                                          boto3_session=boto3_session,
                                                          process_execution_id=process_execution_id)
        steps_df=pd.DataFrame(response['steps'])
        steps_df['jobs_execution_id'] = job_execution_id
        mysql_conn=mysql.MysqlDbManager()
        mysql_conn.insert_dataframe(df=steps_df,table_name='jobs_execution_detail')
        if response['execution_status'] is False and response['is_all_corrupt_record'] is True :
            pass
        elif response['execution_status'] is False:
            raise RuntimeError( f"{glue_table} Failed while updating glue table" )
        else:
            datatype = wr.catalog.get_table_types(database=glue_database, table=sanitize_name(glue_table),boto3_session=boto3_session)
            metadata_df = pd.DataFrame(list(datatype.items()), columns=['COLUMN_NAME', 'DATA_TYPE'])
            metadata_df.drop(metadata_df[metadata_df['COLUMN_NAME'] == 'year'].index, inplace = True)
            metadata_df.drop(metadata_df[metadata_df['COLUMN_NAME'] == 'month'].index, inplace = True)
            metadata_df.drop(metadata_df[metadata_df['COLUMN_NAME'] == 'day'].index, inplace = True)
            metadata_df.drop(metadata_df[metadata_df['COLUMN_NAME'] == 'hour'].index, inplace = True)
            metadata_df.drop(metadata_df[metadata_df['COLUMN_NAME'] == 'process_execution_id'].index, inplace = True)
            metadata_df['ENTITY'] = sanitize_name(glue_table)
            metadata_df['PROCESS_EXECUTION_ID'] = process_execution_id
            conn = snowflake.snowflakeDBManager().get_raw_connection()
            snowflake_prop= snowflake_property()

            success, nchunks, nrows, output = write_pandas(conn=conn, df=metadata_df, table_name=snowflake_prop.entity_schema_detail_table,database=snowflake_prop.database, schema=snowflake_prop.matadata_schema)
            conn.commit()
            entity_process_execution_mapping_table=config.get_config('METADATA','entity_process_execution_mapping_table')
            entity_process_execution_mapping_sql=f"insert into {entity_process_execution_mapping_table}(process_execution_id,entity) values ({process_execution_id},'{glue_table}')"
            print(entity_process_execution_mapping_sql)
            mysql_conn.execute_sql(entity_process_execution_mapping_sql)
            wr.s3.delete_objects(path,boto3_session=boto3_session)

    except Exception as e:
        logger.exception(e)
        refreshDatalaketable_status=False

    return refreshDatalaketable_status


def main(glue_database:str,
                    s3_bucket:str,
                    source_path:str,
                    source_file_format:str,
                    datalake_path:str,
                    process_execution_id:int,
                    job_execution_id:int,
                    datalake_file_format:Optional[str]=None,
                    datalake_write_mode :Optional[str]=None,
                    datalake_file_compression:Optional[str]=None,
                    datalake_partition_cols:Optional[List[str]]=None,
                    partition_base_column:Optional[str]=None,
                    exclusion_entity_list:Optional[str]=None
                    ):
    job_status=True
    boto3_session =  services.botoServices.boto3_session()

    s3_bucket=s3_bucket.replace('/','')
    source_path=source_path.replace('/', '', -1)  if source_path.endswith('/') else source_path
    source_path=source_path.replace('/', '', 1)  if source_path.startswith('/') else source_path
    datalake_path=datalake_path.replace('/', '', -1)  if datalake_path.endswith('/') else datalake_path
    datalake_path_prefix=datalake_path.replace('/', '', 1)  if datalake_path.startswith('/') else datalake_path
    datalake_partition_cols=datalake_partition_cols.split(',') if isinstance(datalake_partition_cols, str) else datalake_partition_cols
    source_path_prefix= f's3://{s3_bucket}/{source_path}/'

    s3_dirctory = wr.s3.list_directories(path=source_path_prefix,boto3_session=boto3_session)
    THREAD_POOL = ThreadPool(processes=int(config.get_config('METADATA','max_thread_count'))) # Define the thread pool to keep track of the sub processes
    thread_result_set={}
    exclusion_entities_list =exclusion_entity_list.split(',') if exclusion_entity_list is not None else []
    #s3_dirctory=['s3://bi-raw-events-cft-dnadev/raw_events/client/']
    for dir in s3_dirctory:
        path= dir
        entity = dir.replace(source_path_prefix, '').split('/')[0]
        if entity not in exclusion_entities_list:
            datalake_path= f's3://{s3_bucket}/{datalake_path_prefix}/{entity}/'
            error_file_path= f's3://{s3_bucket}/error/{entity}/'
            if datalake_partition_cols:
                if partition_base_column is None:
                    raise Exception(f'partition_base_column is not defined to derived {datalake_partition_cols} columns')

            result_file_list = wr.s3.list_objects(path=path,boto3_session=boto3_session )
            if len(result_file_list) > 0  :

                arg_dict ={ 'glue_database':glue_database,
                            'glue_table':entity,
                            'path':path,
                            'source_file_format':source_file_format,
                            'datalake_path':datalake_path,
                            'rejected_records_file_path':error_file_path,
                            'glue_file_format':datalake_file_format,
                            'mode':datalake_write_mode,
                            'compression':datalake_file_compression,
                            'partition_cols':datalake_partition_cols,
                            'partition_base_column':partition_base_column,
                            'process_execution_id':process_execution_id,
                            'boto3_session':boto3_session,
                            'job_execution_id':job_execution_id }

                thread_result_set[entity] =THREAD_POOL.apply_async(refreshDatalaketable,kwds=arg_dict)

            else:
                logger.info(entity+" - No files exist for Reading")

    THREAD_POOL.close() # After all threads started we close the pool
    THREAD_POOL.join() # And wait until all threads are done

    for thread in thread_result_set:
        try:
            print (thread_result_set[thread].get())
            if thread_result_set[thread].get() is False:
                job_status=False
        except Exception as e:

            logger.exception(e)

    return job_status
