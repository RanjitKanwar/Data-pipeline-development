

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 11 16:10:17 2020

@author: manishkundu
"""
import configparser
import os
import boto3
import io
from urllib.parse import urlparse
from typing import Any, Callable, Dict, List, NamedTuple, Optional, Tuple, Type, Union, cast
import logging
from datapipeline import exceptions

_logger: logging.Logger = logging.getLogger(__name__)

class Singleton(type):
    """  Singleton metaclass  """
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class _Config(metaclass=Singleton):
    """  Singleton class  to initialize  application  configuration   """
    __CONFIG_ITEM_LIST : Dict[str, Dict] = {}
    __config_file_path : str = None
    def __set_config(self,config_file_path: str =None):
        """  initialize  configuration   """
        _logger.info("Reading config file :"+config_file_path)
        config = configparser.RawConfigParser()
        config.optionxform = str
        if config_file_path is None:
            _logger.error("")
        if config_file_path !=None :
            if config_file_path.startswith('s3://'):
                o = urlparse(config_file_path, allow_fragments=False)
                self.__read_s3config_file(config,o.netloc,o.path[1:])
            else:
                self.__read_config_file(config,config_file_path)

            for each_section in config.sections():
                self.__CONFIG_ITEM_LIST[each_section] = dict(config._sections[each_section])
    def __read_config_file(self,config, path):
        """ config parser set configuration from config file  """

        return config.read(path)
    def __read_s3config_file(self,config,bucket,key):
        """ config parser set configuration from s3 config file  """
        s3_boto = boto3.client('s3')
        obj = s3_boto.get_object(Bucket=bucket, Key=key)
        return config.read_string(obj['Body'].read().decode())
    def set_config(self, config_file_path : str) ->None:
        self.__config_file_path=config_file_path
        self.__set_config(self.__config_file_path)
    def get_section(self, section : str =None) ->Any:
        """ reurtn  config value based on input param"""
        section_value=None
        if len(self.__CONFIG_ITEM_LIST) == 0:
            raise exceptions.InvalidConfiguration("configuration Not available ,Set configiration using set_config method")
        else :
            config_value=None

            if section is not None:
                section_value=self.__CONFIG_ITEM_LIST.get(section)
        return section_value
    def get_config(self, section : str =None,key : str =None) ->str:
        """ reurtn  config value based on input param"""
        if len(self.__CONFIG_ITEM_LIST) == 0:
                raise exceptions.InvalidConfiguration("configuration Not available ,Set configiration using set_config method")
        else :
            config_value=None

            if section is not None and key  is not None :
                section_value=self.__CONFIG_ITEM_LIST.get(section)
                if section_value is None:
                    raise exceptions.InvalidConfiguration(f"configuration Not available set for section : {section}")
                else :
                    config_value=section_value.get(key)
                    if config_value is None:
                        raise exceptions.InvalidConfiguration(f"configuration missing for section : {section} and key : {key}")

                    else :
                        return config_value
            else:
                raise exceptions.InvalidArgument("Invalid method call")

config: _Config = _Config()
