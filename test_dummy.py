import datetime
from dbtruntype import DbtRunType
from mock_model import MockedTable, SnowflakeRowMock


def test_double_events_are_filtered_out():
    email_id = 'Email to Jef'

    # Snowflake variants: dictionary maps to object and array to array
    event_content = { 'correlation_id': 1, 'properties' : [ { 'data_origin': 'Service.Mail', 'email_provider': 'SendGrid' } ] }

    email_events_table_mock = MockedTable.from_dict('stg_email_events',
            SnowflakeRowMock(email_status = 'sent', event_timestamp = datetime.datetime.now() - datetime.timedelta(hours=8), email_id=email_id,
                             event_content=event_content),
            SnowflakeRowMock(email_status = 'sent', event_timestamp = datetime.datetime.now() - datetime.timedelta(hours=8), email_id=email_id,
                             event_content=event_content),
            SnowflakeRowMock(email_status = 'answered', event_timestamp = datetime.datetime.now() - datetime.timedelta(hours=8), email_id=email_id,
                             event_content=event_content)
    )

    actual = MockedTable.from_mocks('fct_email_events', DbtRunType.FULLREFRESH, email_events_table_mock)

    # Assert that the actual result has the expected number of rows
    assert len(actual.df) == 2

    # Check that only these events are in the event_types
    assert set(actual.df['event_type']) == {'send', 'answered'}

    # Check that both events for first mail and reminder are generated
    assert 'SendGrid' in actual.df['event_content']
    assert 'Service.Mail' in actual.df['event_content']