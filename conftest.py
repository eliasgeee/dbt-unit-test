import subprocess
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import os
from snowflake_connection import SnowflakeConnection

# content of conftest.py
def pytest_configure(config):
    """
    Allows plugins and conftest files to perform initial configuration.
    This hook is called for every plugin and initial conftest
    file after command line options have been parsed.
    """
def pytest_sessionstart(session):
    from mock_model import on_test_run_start
    """
    Called after the Session object has been created and
    before performing collection and entering the run test loop.
    """
    SnowflakeConnection.new()

    on_test_run_start()

def pytest_sessionfinish(session, exitstatus):
    """
    Called after whole test run finished, right before
    returning the exit status to the system.
    """
    SnowflakeConnection.close()
def pytest_unconfigure(config):
    """
    called before test process is exited.
    """
