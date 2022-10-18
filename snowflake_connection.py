import os
import snowflake.connector

class Singleton:
    """Singleton class/attribute"""
    _shared_state = {}

    def __init__(self):
        self.__dict__ = self._shared_state

    def __getattr__(self, key):
        if key in self._shared_state:
            return self._shared_state[key]

class SnowflakeConnection(Singleton):
    def __init__(self) -> None:
        Singleton.__init__(self)

    # Manually manage connection
    @classmethod
    def new(cls):
        print('Opened snowflake connection')

        conn = snowflake.connector.connect()

        SnowflakeConnection().conn = conn

    @classmethod
    def close(cls):
        if SnowflakeConnection().conn is not None:
            print('Closed snowflake connection')
            SnowflakeConnection().conn.close()

    # Automatically manage connection
    def __enter__(self):
        SnowflakeConnection.new()
  
    def __exit__(self):
        SnowflakeConnection.close()
