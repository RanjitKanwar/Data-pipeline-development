from datapipeline import config
from sqlalchemy.pool import QueuePool
from sqlalchemy import create_engine
from datapipeline._properties import mysql_property
import pandas as pd
import logging
logger: logging.Logger = logging.getLogger(__name__)
class Singleton(type):
    """  Singleton metaclass  """
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

class MysqlDbManager(metaclass=Singleton):
    __connection_pool = None

    def __init__(self):
        mysql_prop=mysql_property()
        host = mysql_prop.host
        port = mysql_prop.port
        database = mysql_prop.database
        user = mysql_prop.user
        password = mysql_prop.password
        conn_str="mysql+pymysql://"+user+":"+password+"@"+host+":"+port+"/"+database+"?charset=utf8mb4"
        try:
            self.__connection_pool = create_engine(conn_str, poolclass=QueuePool )
        except Exception as err:
            logger.exception(err)
            raise Exception("Something went wrong: {}".format(err))


    def get_connection(self):
        return self.__connection_pool.raw_connection()

    def get_conn(self):
        return self.__connection_pool.connect()

    def execute_sql(self, sql_query):
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute(sql_query)
            conn.commit()
            conn.close()
        except Exception as err:
            logger.error("Something went wrong: {}".format(err))
            raise Exception(err)
    def executemany_sql(self, sql_query,values):
        try:
            print(sql_query)
            print (values)
            conn = self.get_connection()
            cur = conn.cursor()
            cur.executemany(sql_query,values)
            conn.commit()
            conn.close()
        except Exception as err:
            logger.error("Something went wrong: {}".format(err))
            raise Exception(err)
    def call_proc(self, proc,args):

        try:
            result_args=None
            argument_count=len(args)
            arg_sql="select "
            for seq in range(argument_count):
                if(seq<argument_count-1):
                    arg_sql=arg_sql+'@_'+proc+"_"+str(seq)+' ,'
                else:
                    arg_sql=arg_sql+'@_'+proc+"_"+str(seq)
            conn = self.get_connection()
            cur = conn.cursor()
            cur.callproc(proc, args)
            cur.execute(arg_sql)
            result_args=list(cur.fetchall()[0])
            conn.close()
            return result_args
        except Exception as err:
            logger.error("Something went wrong: {}".format(err))
            raise Exception(err)

    def fetch_dataframe(self,sql_query):
        try:
            conn = self.get_connection()
            if(conn):
                data = pd.read_sql_query(sql_query, conn)
                conn.close()
                return data
        except Exception as err:
            logger.error("Something went wrong: {}".format(err))
            raise Exception(err)

    def insert_dataframe(self,df,table_name):
        try:
            conn = self.get_conn()
            if(conn):
                df.to_sql(name=table_name,con=conn, if_exists='append',index=False)
                conn.close()
        except Exception as err:
            logger.error("Something went wrong: {}".format(err))
            raise Exception(err)


    def close_connection(self, conn):
        if (conn.is_connected()):
            conn.close()

