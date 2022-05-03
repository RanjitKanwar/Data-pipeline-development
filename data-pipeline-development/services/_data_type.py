"""Internal (private) Data Types Module."""

import datetime
import logging
import re

from typing import Any, Dict, List, Match, Optional, Sequence, Tuple

import pyspark.sql.dataframe
def find_glue_dtype(dtype)-> str:
    if dtype=='int':
        return "bigint"
    else :
        return dtype





def cast_spark_dataframe_with_glue_types(df:pyspark.sql.dataframe, spark_data_frame_columns: Dict[str,str],glue_table_columns: Dict[str,str] =None):
    existing_columns_list=[]
    new_column_list=[]
    glue_table_columns = glue_table_columns if glue_table_columns else {}
    for col,dtype in spark_data_frame_columns.items():
        if col in glue_table_columns.keys():
            glue_dtype=glue_table_columns[col]
            key = col if dtype == glue_dtype else \
                f'cast({col} as {glue_dtype}) {col}' if  glue_dtype != "double" else f'cast({col} as decimal(38,6)) {col}'
            existing_columns_list.append(key)
        else:
            key= col if dtype != "double" else f'cast({col} as decimal(38,6)) {col}'
            new_column_list.append(key)

    return df.selectExpr(existing_columns_list+new_column_list)

