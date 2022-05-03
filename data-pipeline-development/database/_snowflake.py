import os
from datapipeline import config
from datapipeline._properties import snowflake_property
from snowflake.sqlalchemy import URL
from sqlalchemy.pool import QueuePool
from sqlalchemy import create_engine
import logging
import pandas as pd
logger: logging.Logger = logging.getLogger(__name__)
class Singleton(type):
    """  Singleton metaclass  """
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]



class snowflakeDBManager(metaclass=Singleton):
    __engine = None

    def __init__(self):
        try:
            snowflake_prop=snowflake_property()
            self.__engine = create_engine(URL(
                user=snowflake_prop.user,
                password=snowflake_prop.password,
                account=snowflake_prop.account,
                database=snowflake_prop.database,
                warehouse=snowflake_prop.warehouse,
                role=snowflake_prop.role,
                schema=snowflake_prop.schema
            ), echo=False, poolclass=QueuePool, pool_pre_ping=True)
        except Exception as err:
            logger.error(str(err))
            raise Exception("Something went wrong: {}".format(err))

    def upload_to_snowflake(self, data_frame,
                            con,
                            table_name,
                            truncate=False,
                            create=False, insert=True):

        file_name = f"{table_name}.csv"
        file_path = os.path.abspath(file_name)
        data_frame.to_csv(file_path, index=False, header=False)

        if create:
            data_frame.head(0).to_sql(name=table_name,
                                      con=con,
                                      if_exists="replace",
                                      index=False)
        if truncate:
            con.execute(f"truncate table {table_name}")

            con.execute(f"put file://{file_path}* @%{table_name}")
            con.execute(f"copy into {table_name}")
        if insert:
            con.execute(f"put file://{file_path}* @%{table_name}")
            con.execute(f"copy into {table_name}")
            con.close()

    def execute_sql(self, sql_query):

        result_args=None
        try:
            conn = self.get_raw_connection()
            cur = conn.cursor()
            cur.execute("alter session set autocommit= true")
            cur.execute(sql_query)
            #print(cur.fetchall())
            result_args=cur.fetchall()
            conn.commit()
            conn.close()
        except Exception as err:
            logger.error(err)
            raise Exception("Something went wrong: {}".format(err))

        return result_args
    def fetch_dataframe(self,sql_query):
        try:
            conn = self.getconn()
            if(conn):
                data = pd.read_sql_query(sql_query, conn)
                conn.close()
                return data
        except Exception as err:
            logger.error("Something went wrong: {}".format(err))
            raise Exception(err)
    def getconn(self):
        if self.__engine is not None:
            connection = self.__engine.connect()
        else:
            snowflakeDBManager()
            connection = self.__engine.connect()
        return connection

    def get_raw_connection(self):
        if self.__engine is not None:
            connection = self.__engine.raw_connection()
        else:
            snowflakeDBManager()
            connection = self.__engine.raw_connection()
        return connection

    def closeAllConnection(self):
        logger.info("closing snowflake connection pool")
        self.__engine.dispose()