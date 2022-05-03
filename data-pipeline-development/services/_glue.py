import  logging
from datapipeline import _utils as utils
from datapipeline import exceptions
from datapipeline.services import _data_type
from datapipeline.services  import _spark as sparkService
import boto3
from  datetime import datetime
from  awswrangler.catalog  import create_parquet_table  as create_glue_table
from  awswrangler.catalog  import add_parquet_partitions  as update_glue_table_partition
from  awswrangler import s3 as awraglerS3Services
import pyspark.sql.dataframe
from typeguard import typechecked
from typing import Optional,Any,Dict,List
from datapipeline._properties import execution_step_property
from pyspark.sql.functions import *
logger: logging.Logger = logging.getLogger(__name__)

def _get_glue_table_defination( database: pyspark.sql.dataframe, table: str, boto3_session: boto3.Session):
    try :
        response_get_table=boto3_session.get_table(
            DatabaseName=database,
            Name=table
        )
        return response_get_table
    except boto3_session.exceptions.EntityNotFoundException:
        return None


@typechecked
def dataframe_to_datalake( df:pyspark.sql.dataframe,database:str, table:str,boto3_session:boto3.Session,datalake_path:str,mode:str,partition_cols:List[str],
                           compression:Optional[str]=None,format:Optional[str]=None) ->List[Dict[str,Any]]:

    #Sanitize dataframe column name
    df = df.toDF(*(utils.sanitize_name(c) for c in df.columns))

    #Glue defination object
    glue_table_defination: Optional[Dict[str, Any]] = None

    # Get glue defination object and validate for data lake path
    if database is not None and table is not None:
        #Get glue client
        glue_session = boto3_session.client('glue')
        glue_table_defination = _get_glue_table_defination(database=database, table=table, boto3_session=glue_session)
        glue_storage_location=glue_table_defination['Table']["StorageDescriptor"]["Location"] if glue_table_defination else None
        if datalake_path is None:
            if glue_storage_location:
                datalake_path = glue_storage_location
            else:
                raise exceptions.InvalidArgumentValue(
                    "Glue table does not exist in the catalog. Please pass the `datalake_path` argument to create it."
                )
        elif datalake_path and glue_storage_location:
            if datalake_path.rstrip("/") != glue_storage_location.rstrip("/"):
                raise exceptions.InvalidArgumentValue(
                    f"The specified path: {datalake_path}, does not match the existing Glue catalog table path: {glue_storage_location}"
                )
    #Glue columns datatype
    glue_table_columns={i['Name']:i['Type']  for i in glue_table_defination['Table']['StorageDescriptor']['Columns']} if glue_table_defination else None

    #Source dataframe column datatype
    spark_data_frame_columns={ col:dtype for col,dtype in df.dtypes }
    #source dataframe datatype casting for glue
    df = _data_type.cast_spark_dataframe_with_glue_types(df,spark_data_frame_columns,glue_table_columns)
    #Write data-lake files on s3 based on partition keys
    partitions_values={}

    try:
        step_datalake_creation=execution_step_property(entity=table,step='Load_datalake',start_dt=datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f'))
        logger.info(f' {table} - writing file on datalake start:')
        sparkService.write_file(df=df,partition_columns=partition_cols,mode=mode,path=datalake_path,compression=compression,format=format)
        logger.info(f' {table} - writing file on datalake end:')
        step_datalake_creation.end_dt=datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
        step_datalake_creation.status='Success'
        columns_types,partitions_types,partitions_values=sparkService.data_frame_columns_partitions_summary(df=df,partition_cols=partition_cols,path=datalake_path)
        logger.info(f' {table} - Datalake parititon :' +str(partitions_values.keys()))

        step_glue_table_update=execution_step_property(entity=table,step='Update_glue_schema',start_dt=datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f'))

        create_glue_table(
            database=database,
            table=table,
            path=datalake_path,
            columns_types=columns_types,
            partitions_types=partitions_types,
            compression=compression,
            mode=mode,
            boto3_session=boto3_session)

        update_glue_table_partition(
            database=database,
            table=table,
            partitions_values=partitions_values,
            boto3_session=boto3_session
        )
        step_glue_table_update.end_dt=datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
        step_glue_table_update.status='Success'
        return [step_datalake_creation.asDict(),step_glue_table_update.asDict()]
    except Exception:
        partitions_keys=[key for key in partitions_values.keys()]
        logger.info("glue table processing failed, cleaning up S3 (paths: %s).", partitions_keys)
        if partitions_keys:
            awraglerS3Services.delete_objects(path=partitions_keys,use_threads=True,boto3_session=boto3_session)
        raise

