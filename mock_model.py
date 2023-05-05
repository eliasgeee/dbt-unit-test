from datetime import datetime, date
from enum import Enum
import json
import os
import subprocess
import traceback
import pandas as pd
from snowflake_connection import SnowflakeConnection
from dbtruntype import DbtRunType
import time

DBT_PROJECT_NAME='hc_dbt'

models_sql_per_run_type={}

def on_test_run_start():
    command_fullrefresh = f'dbt compile --profiles-dir . --target DEV --full-refresh'
    process_fullrefresh = subprocess.Popen(command_fullrefresh.split(), stdout=subprocess.PIPE)
    _, error_fullrefresh = process_fullrefresh.communicate()

    if error_fullrefresh:
        raise Exception('Something went wrong when trying to compile DBT')
    
    with open("target/manifest.json") as f1:
        manifest_fullref = json.load(f1)

        for _,model in manifest_fullref["nodes"].items():
            name = model['name']
            models_sql_per_run_type[f'{name}:{DbtRunType.FULLREFRESH}'] = model['compiled_sql']

    command_incremental = f'dbt compile --profiles-dir . --target DEV'
    process_incremental = subprocess.Popen(command_incremental.split(), stdout=subprocess.PIPE)
    _, error_incremental = process_incremental.communicate()

    if error_incremental:
        raise Exception('Something went wrong when trying to compile DBT incrementally')

    with open("target/manifest.json") as f2:
        manifest_inc = json.load(f2)

        for _,model in manifest_inc["nodes"].items():
            name = model['name']
            models_sql_per_run_type[f'{name}:{DbtRunType.INCREMENTAL}'] = model['compiled_sql']

    print("Loaded compiled sql")

class SnowflakeRowMock:
    def __init__(self, **kwargs) -> None:
        for columnName, columnValue in kwargs.items():
            self.__dict__[columnName] = columnValue

class SnowflakeTable:
    @classmethod
    def create(cls, *args):
        table = []
        for arg in args:
            # filter out standard props in object dict
            row = {key: value for (key, value) in arg.__dict__.items() if not key.startswith('_')}
            table.append(row)
        return table

class MockType(Enum):
    dictionary = 0
    dataframe = 1

class SnowflakeTypeMapping:
    # datatype + if a value needs to be displayed between single quotes
    mapping = {
        str(str): ('text', True),
        str(int): ('number', False),
        str(bool): ('boolean', False),
        str(datetime): ('datetime', True),
        str(date): ('datetime', True),
        str(float): ('decimal', False),
        str(dict): ('object', False),
        str(list): ('array', False)
    }

    @classmethod
    def cast_as_snowflake_value(cls, val) -> str:
        if val is None:
            return 'NULL'
        val_type = str(type(val))
        if val_type in cls.mapping:
            display_val = val
            snowflake_mapping, display_between_quotation_marks = cls.mapping[val_type]
            if display_between_quotation_marks:
                display_val = f'\'{display_val}\''
            if snowflake_mapping == 'object':
                returnValue = 'TO_VARIANT(OBJECT_CONSTRUCT('
                for index, (key, value) in  enumerate(val.items()):
                    returnValue += f'\'{key}\',{cls.cast_as_snowflake_value(value)}'
                    if index != len(val) - 1:
                        returnValue += ','
                returnValue += '))'
                return returnValue
            if snowflake_mapping == 'array':
                returnValue = 'TO_VARIANT(ARRAY_CONSTRUCT('
                for index, value in  enumerate(val):
                    returnValue += f'{cls.cast_as_snowflake_value(value)}'
                    if index != len(val) - 1:
                        returnValue += ','
                returnValue += '))'
                return returnValue
            return f'{display_val}::{snowflake_mapping}'
        else:
            raise Exception(
                f'Unknown type {val_type} in {SnowflakeTypeMapping}. Please register the type in the mapping dictionary')

class MockedTable:
    def __init__(self, model_name: str, mock_type: MockType, list_of_dicts=None, df: pd.DataFrame = None):
        self.mock_type = mock_type
        self.model_name = model_name
        self.list_of_dicts = list_of_dicts
        self.df = df
    
    def to_mock_query_string(self) -> str:
        sql = '('
        if self.mock_type == MockType.dictionary:
            pos = -1
            for entry in self.list_of_dicts:
                pos += 1
                sql = f'{sql} SELECT'
                pos_in_select = -1
                items = entry.items()
                for key, value in items:
                    pos_in_select += 1
                    sql = f'{sql} {SnowflakeTypeMapping.cast_as_snowflake_value(value)} as {key}'
                    if pos_in_select != len(items) - 1:
                        sql = f'{sql}, '
                if pos != len(self.list_of_dicts) - 1:
                    sql = f'{sql} \n UNION ALL \n'
            sql = f'{sql})'
            return sql
        else:
            raise Exception(f'Not supported: {self.mock_type}')

    # @classmethod
    # def from_dict(cls, model_name:str, table:list) -> "MockModel":
    #    return MockModel(model_name, mock_type=MockType.dictionary, list_of_dicts=table)
    @classmethod
    def from_dict(cls, model_name: str, *rows: list) -> "MockedTable":
        table = []
        for arg in rows:
            # filter out standard props in object dict
            row = {key: value for (key, value) in arg.__dict__.items() if not key.startswith('_')}
            table.append(row)
        return MockedTable(model_name, mock_type=MockType.dictionary, list_of_dicts=table)

    @classmethod
    def from_mocks(cls, model_name: str, dbt_run_type: DbtRunType, *mocks) -> "MockedTable":
        with open("target/manifest.json") as f:
            manifest = json.load(f)
            model_node = manifest["nodes"][f'model.{DBT_PROJECT_NAME}.{model_name}']
            dependencies = manifest["nodes"][f'model.{DBT_PROJECT_NAME}.{model_name}']['depends_on']['nodes']

            n = model_node['name']
            compiled_sql = models_sql_per_run_type[f'{n}:{dbt_run_type}']
            compiled_sql = compiled_sql.lower()
            conn = SnowflakeConnection().conn
            try:
                for mock in mocks:
                    if not isinstance(mock, MockedTable):
                        raise Exception(f'{mock} is not of type {MockedTable}')
                
                    if mock.model_name == model_name:
                        db = model_node['database']
                        schema = model_node['schema']
                        m_name = model_node['name']
                    else:
                        mock_node_name = next(
                        dependency_name for dependency_name in dependencies if mock.model_name in dependency_name)
                        mock_node = None 
                    
                        if mock_node_name.startswith("source"):
                            mock_node = manifest["sources"][mock_node_name]
                        else:
                            mock_node = manifest["nodes"][mock_node_name]

                        if mock_node is None:
                            raise Exception(f'Could not find dependency: {mock_node_name}')
                        db = mock_node['database']
                        schema = mock_node['schema']
                        m_name = mock_node['name']

                    table_name = f'{db}.{schema}.{m_name}'.lower()
                    compiled_sql = compiled_sql.replace(table_name, mock.to_mock_query_string())
                df = pd.read_sql(compiled_sql, conn)
                print(f'Executed query: \n {compiled_sql} \n')
                # Lowercase column names from Snowflake
                df.columns = [column.lower() for column in df.columns]
                pd.set_option('display.max_rows', None)
                pd.set_option('display.max_columns', 5)
                pd.set_option('display.width', 5000)
                pd.set_option('display.colheader_justify', 'center')
                pd.set_option('display.precision', 3)
                print('\nActual:')
                print(df)
                return MockedTable(model_name, mock_type=MockType.dataframe, df=df)
            except Exception as e:
                print(f'{e} {traceback.format_exc()}')
                if 'duplicate alias \'values\'' in e.args[0]:
                    print('Give each table in the query an alias if it uses a ref')
                raise
            finally:
                pass