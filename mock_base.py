from datetime import datetime,date
from enum import Enum
import json
import os
import subprocess
import traceback

import pandas as pd

from snowflake_connection import SnowflakeConnection
from dbtruntype import DbtRunType
from IPython.display import display

class MockType(Enum):
    dictionary=0
    dataframe=1

class SnowflakeTypeMapping:
    # datatype + if a value needs to be displayed between single quotes
    mapping = {
        str(str): ('text', True),
        str(int): ('number', False),
        str(bool): ('boolean', False),
        str(datetime): ('datetime', True),
        str(date): ('datetime', True)
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

            return f'{display_val}::{snowflake_mapping}'
        else:
            raise Exception(f'Unknown type {val_type} in {SnowflakeTypeMapping}. Please register the type in the mapping dictionary')


class MockBase:
    def __init__(self, model_name:str, mock_type:MockType, list_of_dicts = None, df:pd.DataFrame=None):
        self.mock_type=mock_type
        self.model_name=model_name
        self.list_of_dicts=list_of_dicts
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
                    pos_in_select+=1

                    sql = f'{sql} {SnowflakeTypeMapping.cast_as_snowflake_value(value)} as {key}'

                    if pos_in_select != len(items) - 1:
                        sql = f'{sql}, '

                if pos != len(self.list_of_dicts) - 1:
                    sql = f'{sql} \n UNION ALL \n'
            sql = f'{sql})'
            return sql

        else:
            raise Exception(f'Not supported: {self.mock_type}')

    @classmethod
    def from_dict(cls, model_name:str, *rows:list) -> "MockBase":
        table = []
        for arg in rows:
            # filter out standard props in object dict
            row = {key: value for (key, value) in arg.__dict__.items() if not key.startswith('_') }
            table.append(row)

        return MockBase(model_name, mock_type=MockType.dictionary, list_of_dicts=table)

    @classmethod
    def from_mocks(cls, model_name:str, dbt_run_type: DbtRunType, *mocks) -> "MockBase":
        command = f'dbt compile --profiles-dir . --target DEV --select {model_name}'

        if dbt_run_type.value == DbtRunType.NORMAL.value:
            command = f'{command} --full-refresh'

        print(f'command: {command}')

        process = subprocess.Popen(command.split(), stdout=subprocess.PIPE)
        output, error = process.communicate()   

        if error:
            raise Exception('Something went wrong when trying to compile DBT')

        with open("target/manifest.json") as f:
            manifest = json.load(f)
            model_node = manifest["nodes"][f'model.hc_dbt.{model_name}']
            dependencies = manifest["nodes"][f'model.hc_dbt.{model_name}']['depends_on']['nodes']

            if 'compiled_sql' not in model_node:
                raise Exception(f'Please run: {command}')

            compiled_sql = model_node['compiled_sql']

            conn = SnowflakeConnection().conn

            try: 
                for mock in mocks:
                    if not isinstance(mock, MockBase):
                        raise Exception(f'{mock} is not of type {MockBase}')

                    mock_node_name = next (dependency_name for dependency_name in dependencies if mock.model_name in dependency_name) 
                    mock_node = manifest["nodes"][mock_node_name]

                    db = mock_node['database']
                    schema = mock_node['schema']
                    m_name = mock_node['name']

                    table_name = f'{db}.{schema}.{m_name}'  

                    compiled_sql = compiled_sql.replace(table_name, mock.to_mock_query_string())

                df = pd.read_sql(compiled_sql, conn)

                # Lowercase column names from Snowflake
                df.columns = [column.lower() for column in df.columns]

                pd.set_option('display.max_rows', None)
                pd.set_option('display.max_columns', 5)
                pd.set_option('display.width', 5000)
                pd.set_option('display.colheader_justify', 'center')
                pd.set_option('display.precision', 3)
                
                print('Actual (with):')
                display(df)

                return MockBase(model_name, mock_type=MockType.dataframe, df=df) 
            except Exception as e:
                print(f'{e} {traceback.format_exc()}')

                if 'duplicate alias \'values\'' in e.args[0]:
                    print('Give each table in the query an alias if it uses a ref')

                raise
            finally:
                pass 
