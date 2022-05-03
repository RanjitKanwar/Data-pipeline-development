#import findspark
#findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.dataframe
import sys
from typing import Optional,Dict,List,Tuple,Any
from datapipeline import config
from pyspark.conf import SparkConf
from datapipeline.services import _data_type as data_type
import logging
from typeguard import typechecked


_logger: logging.Logger = logging.getLogger(__name__)
def get_spark_session(sparkConfig:Optional[Dict[str,any]] = None):
    conf=SparkConf()
    spark_env_conf=None
    try:
        spark_env_conf=config.get_section('SPARK_CONFIG')
    except Exception as e:
        pass
    if spark_env_conf is not None:
        for key, value in spark_env_conf.items():
            conf.set(key=key,value=value)
    if sparkConfig is not None:
        for key, value in sparkConfig.items():
            conf.set(key=key,value=value)
    _logger.info ("Spark Session properties:")
    for key, value in conf.getAll():
        _logger.info (key + "=" +value)
    return SparkSession.builder \
        .config(conf=conf)\
        .getOrCreate()


def terminate_spark_session(session):
        session.stop()
        return True
from pyspark.sql.types import *

# Auxiliar functions
@typechecked
def drive_year_month_day_hour_columns(df:pyspark.sql.dataframe,column:str):

    partition_df=df.withColumn("year",
                     year(col(column))).withColumn("month",
                     month(col(column))).withColumn("day",
                     dayofmonth(col(column))).withColumn("hour",
                     hour(col(column)))
    return partition_df
@typechecked
def load_data( path :str ,spark_session :SparkSession,format:str ):
    source_file_path = path.replace("s3://","s3a://") if path.startswith('s3://')  else path
    print (f"reading file from {source_file_path}")
    return spark_session.read.option("inferSchema", "true").option("mode", "PERMISSIVE").option("corrupted_record", "_corrupt_record").format(format).load(source_file_path).withColumn("source_file_name",input_file_name())
@typechecked
def write_file(df: pyspark.sql.dataframe, path:str , format:Optional[str]=None , partition_columns :Optional[List[str]] =None,compression:Optional[str]=None,mode:Optional[str]=None):
    #validation S3 path
    datalake_path = path.replace("s3://","s3a://") if path.startswith('s3://')  else path
    format= format if format else 'parquet'
    mode= mode if mode else 'overwrite'
    df = df.repartition(*partition_columns) if partition_columns is not None else df
    df.write.mode(mode).format(format).save(path=datalake_path,partitionBy=partition_columns,compression=compression)

@typechecked
def dataframe_records_count_column_wise(df:pyspark.sql.dataframe)->Dict[str,Any]:
    #check for corrput record
    df_summary=df.summary("count")
    column_records_counts=df_summary.toPandas().set_index('summary').T.to_dict()
    return column_records_counts["count"]


def data_frame_columns_partitions_summary ( df:pyspark.sql.dataframe,
                                             partition_cols:List[str],
                                             path:str) -> Tuple[Dict[str,Dict[str,str]],Dict[str,Dict[str,str]], Dict[str,Dict[str,List[str]]]]:


    root_path =  path if path.endswith('/')  else f"{path}/"
    partitions_values: Dict[str,List[str]] = {}
    columns_types= {col : data_type.find_glue_dtype(dtype)  for col,dtype in df.dtypes if col not in partition_cols  }
    partitions_types= {col : data_type.find_glue_dtype(dtype)  for col,dtype in df.dtypes if col  in partition_cols  }

    partition_detail_df=df.dropDuplicates(partition_cols).select(partition_cols)
    for row in partition_detail_df.collect():
        subdir = "/".join([f"{name}={val}" for name, val in zip(partition_cols, row)])
        prefix: str = f"{root_path}{subdir}/"
        partitions_values[prefix] = [str(k) for k in row]

    return columns_types,partitions_types,partitions_values