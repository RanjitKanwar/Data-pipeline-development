from datapipeline._config import config
from typing import Any,Optional
class Singleton(type):
    """  Singleton metaclass  """
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]
class execution_step_property():
    def __init__(self,entity:Optional[str]=None,
                 step:Optional[str]=None,
                 end_dt:Optional[str]=None,
                 rejected_records_cnt:Optional[int]=None,
                 records_cnt:Optional[int]=None,
                 description:Optional[str]=None,
                 status:Optional[str]=None,
                 start_dt:Optional[str]=None):
        self.entity=entity
        self.step=step
        self.start_dt=start_dt
        self.end_dt=end_dt
        self.rejected_records_cnt=rejected_records_cnt
        self.records_cnt=records_cnt
        self.description=description
        self.status=status


    @property
    def entity(self):
        return self.__entity

    @entity.setter
    def entity(self, val):
            self.__entity = val

    @property
    def step(self):
        return self.__step

    @step.setter
    def step(self, val):
        self.__step = val


    @property
    def start_dt(self):
        return self.__start_dt

    @start_dt.setter
    def start_dt(self, val):
        self.__start_dt = val


    @property
    def end_dt(self):
        return self.__end_dt

    @end_dt.setter
    def end_dt(self, val):
        self.__end_dt= val

    @property
    def rejected_records_cnt(self):
        return self.__rejected_records_cnt

    @rejected_records_cnt.setter
    def rejected_records_cnt(self, val):
        self.__rejected_records_cnt = val


    @property
    def records_cnt(self):
        return self.__records_cnt

    @records_cnt.setter
    def records_cnt(self, val):
        self.__records_cnt = val


    @property
    def description(self):
        return self.__description

    @description.setter
    def description(self, val):
        self.__description = val

    @property
    def status(self):
        return self.__status

    @status.setter
    def status(self, val):
        self.__status = val

    def asDict(self):
        return {"entity":self.entity,"step":self.step,"start_dt":self.start_dt,"end_dt":self.end_dt,"reject_records_cnt":self.rejected_records_cnt,"records_cnt":self.records_cnt,"description":self.description,"status":self.status}

        account = config.get_item(application_subscriber,"sf_account")
        database = config.get_item(application_subscriber,"sf_database")
        warehouse = config.get_item(application_subscriber,"sf_warehouse")
        user = config.get_item(application_subscriber,"sf_user")
        password = config.get_item(application_subscriber,"sf_password")
        role = config.get_item(application_subscriber,"sf_role")
        schema = config.get_item(application_subscriber,"sf_schema")

class snowflake_property(metaclass=Singleton):
    def __init__(self,
                 account:Optional[str]=None,
                 database:Optional[str]=None,
                 warehouse:Optional[str]=None,
                 user:Optional[int]=None,
                 password:Optional[int]=None,
                 role:Optional[str]=None,
                 schema:Optional[str]=None,
                matadata_schema:Optional[str]=None,
                 entity_schema_detail_table:Optional[str]=None):
        self.account=account
        self.database=database
        self.warehouse=warehouse
        self.user=user
        self.password=password
        self.role=role
        self.schema=schema
        self.matadata_schema=matadata_schema
        self.entity_schema_detail_table=entity_schema_detail_table


    @property
    def account(self):
        return self.__account

    @account.setter
    def account(self, val):
        self.__account = val

    @property
    def database(self):
        return self.__database

    @database.setter
    def database(self, val):
        self.__database = val


    @property
    def warehouse(self):
        return self.__warehouse

    @warehouse.setter
    def warehouse(self, val):
        self.__warehouse = val


    @property
    def user(self):
        return self.__user

    @user.setter
    def user(self, val):
        self.__user= val

    @property
    def password(self):
        return self.__password

    @password.setter
    def password(self, val):
        self.__password = val


    @property
    def role(self):
        return self.__role

    @role.setter
    def role(self, val):
        self.__role = val


    @property
    def schema(self):
        return self.__schema

    @schema.setter
    def schema(self, val):
        self.__schema = val

    @property
    def matadata_schema(self):
        return self.__matadata_schema

    @matadata_schema.setter
    def matadata_schema(self, val):
        self.__matadata_schema = val
    @property
    def entity_schema_detail_table(self):
        return self.__entity_schema_detail_table

    @entity_schema_detail_table.setter
    def entity_schema_detail_table(self, val):
        self.__entity_schema_detail_table = val

class mysql_property(metaclass=Singleton):
    def __init__(self,
                 host:Optional[str]=None,
                 port:Optional[str]=None,
                 database:Optional[str]=None,
                 user:Optional[int]=None,
                 password:Optional[int]=None):
        self.host=host
        self.database=database
        self.port=port
        self.user=user
        self.password=password


    @property
    def host(self):
        return self.__host

    @host.setter
    def host(self, val):
        self.__host= val

    @property
    def database(self):
        return self.__database

    @database.setter
    def database(self, val):
        self.__database = val


    @property
    def port(self):
        return self.__port

    @port.setter
    def port(self, val):
        self.__port= val


    @property
    def user(self):
        return self.__user

    @user.setter
    def user(self, val):
        self.__user= val

    @property
    def password(self):
        return self.__password

    @password.setter
    def password(self, val):
        self.__password = val



def set_property(property_class,section):
        if property_class =="SNOWFLAKE":
            section=section.upper()
            sf_prop=snowflake_property()
            sf_prop.user=config.get_config(section,'user')
            sf_prop.password=config.get_config(section,'password')
            sf_prop.warehouse=config.get_config(section,'warehouse')
            sf_prop.account=config.get_config(section,'account')
            sf_prop.database=config.get_config(section,'database')
            sf_prop.role=config.get_config(section,'role')
            sf_prop.schema=config.get_config(section,'schema')
            sf_prop.entity_schema_detail_table=config.get_config(section,'entity_schema_detail_table')
            sf_prop.matadata_schema=config.get_config(section,'matadata_schema')

        if property_class =="MYSQL":
            section=section.upper()
            mysql_prop=mysql_property()
            mysql_prop.host=config.get_config(section,'host')
            mysql_prop.user=config.get_config(section,'user')
            mysql_prop.password=config.get_config(section,'password')
            mysql_prop.port=config.get_config(section,'port')
            mysql_prop.database=config.get_config(section,'database')



