from datapipeline import config
from datapipeline.metadata import _metastore as metastoreFunc
from datapipeline.database import _mysql as mysql
from datapipeline.database import _snowflake as snowflake
from multiprocessing.pool import ThreadPool
import logging
import pandas as pd

logger: logging.Logger = logging.getLogger(__name__)

def refreshEntityTimelineTable(entity:str,
                             sf_proc:str,
                             process_execution_id:int,
                             job_execution_id:int ):
    refreshEntityTimelineTableStatus=True
    try :

        logger.info(f'{entity} : Refresh timeline layer start')
        sqlquery=f"call {sf_proc}('{entity}')"
        logger.info(f'{entity} : call Statement : {sqlquery} ')
        sf_conn=snowflake.snowflakeDBManager()
        result=sf_conn.execute_sql(sqlquery)
        result_list=list(eval(result[0][0]))
        print (result_list)
        if len(result_list)>0:
            logger.info(f'{entity} - loading job_execution_detail table  start')
            job_execution_details_df = pd.DataFrame(result_list, columns=['entity','step','start_dt','end_dt','status','description'])
            job_execution_details_df["jobs_execution_id"]=job_execution_id
            job_execution_details_df['status'] = job_execution_details_df['status'].apply(str)
            if job_execution_details_df['status'].str.contains('Failed').any():
                refreshEntityTimelineTableStatus=False
                raise Exception(f'{entity} : {sqlquery} failed')
            #record_count=s3_processed_files_df.loc[s3_processed_files_df['step'] == 'History_loaded',['step_count']].sum()
            mysql_conn=mysql.MysqlDbManager()
            mysql_conn.insert_dataframe(df=job_execution_details_df.sort_values('start_dt'),table_name="jobs_execution_detail")
            logger.info(f'{entity} : Refresh timeline layer ended successfully')

    except Exception as e:
        logger.error(f'{entity} : Refresh timeline layer  failed')
        logger.error(e)
        refreshEntityTimelineTableStatus=False

    return refreshEntityTimelineTableStatus


def refreshEntityTimeline(  process_execution_id:int,
           job_execution_id:int,
           sf_proc:str
           ):
    job_status=True
    try:
        THREAD_POOL = ThreadPool(processes=int(config.get_config('METADATA','max_thread_count'))) # Define the thread pool to keep track of the sub processes
        thread_result_set={}
        history_entity_list =metastoreFunc.get_timeline_entity_list()
        for entity in history_entity_list:
            arg_dict ={ 'entity':entity,
                        'sf_proc':sf_proc,
                        'process_execution_id':process_execution_id,
                        'job_execution_id':job_execution_id }

            thread_result_set[entity] =THREAD_POOL.apply_async(refreshEntityTimelineTable,kwds=arg_dict)

        THREAD_POOL.close() # After all threads started we close the pool
        THREAD_POOL.join() # And wait until all threads are done

        for thread in thread_result_set:
            try:
                if thread_result_set[thread].get() is False:
                    job_status=False
            except Exception as e:
                job_status=False
                logger.exception(e)
    except Exception as e:
        job_status=False
        logger.exception(e)
    return job_status

def refreshEntityContainerTimelineTable(entity:str,
                               sf_proc:str,
                               process_execution_id:int,
                               job_execution_id:int ):
    refreshEntityContainerTimelineTableStatus=True
    try :

        logger.info(f'{entity} : Refresh timeline layer start')
        sqlquery=f"call {sf_proc}('{entity}')"
        logger.info(f'{entity} : call Statement : {sqlquery} ')
        sf_conn=snowflake.snowflakeDBManager()
        result=sf_conn.execute_sql(sqlquery)
        result_list=list(eval(result[0][0]))
        print (result_list)
        if len(result_list)>0:
            logger.info(f'{entity} - loading job_execution_detail table  start')
            job_execution_details_df = pd.DataFrame(result_list, columns=['entity','step','start_dt','end_dt','status','description'])
            job_execution_details_df["jobs_execution_id"]=job_execution_id
            job_execution_details_df['status'] = job_execution_details_df['status'].apply(str)
            if job_execution_details_df['status'].str.contains('Failed').any():
                refreshEntityContainerTimelineTableStatus=False
                raise Exception(f'{entity} : {sqlquery} failed')
            #record_count=s3_processed_files_df.loc[s3_processed_files_df['step'] == 'History_loaded',['step_count']].sum()
            mysql_conn=mysql.MysqlDbManager()
            mysql_conn.insert_dataframe(df=job_execution_details_df.sort_values('start_dt'),table_name="jobs_execution_detail")
            logger.info(f'{entity} : Refresh timeline layer ended successfully')

    except Exception as e:
        logger.error(f'{entity} : Refresh timeline layer  failed')
        logger.error(e)
        refreshEntityContainerTimelineTableStatus=False

    return refreshEntityContainerTimelineTableStatus


def refreshEntityContainerTimeline(  process_execution_id:int,
                            job_execution_id:int,
                            sf_proc:str
                            ):
    job_status=True
    try:
        THREAD_POOL = ThreadPool(processes=int(config.get_config('METADATA','max_thread_count'))) # Define the thread pool to keep track of the sub processes
        thread_result_set={}
        history_entity_list =metastoreFunc.get_timeline_entity_list()
        for entity in history_entity_list:
            arg_dict ={ 'entity':entity,
                        'sf_proc':sf_proc,
                        'process_execution_id':process_execution_id,
                        'job_execution_id':job_execution_id }

            thread_result_set[entity] =THREAD_POOL.apply_async(refreshEntityContainerTimelineTable,kwds=arg_dict)

        THREAD_POOL.close() # After all threads started we close the pool
        THREAD_POOL.join() # And wait until all threads are done

        for thread in thread_result_set:
            try:
                if thread_result_set[thread].get() is False:
                    job_status=False
            except Exception as e:
                job_status=False
                logger.exception(e)
    except Exception as e:
        job_status=False
        logger.exception(e)
    return job_status