import pytest
from copy import deepcopy
from target_postgres.db_sync import DbSync, column_type, flatten_record, flatten_schema

BASE_SCHEMA = {
    'type': 'SCHEMA',
    'stream': 'tests',
    'schema': {
        'properties': {
            'id': {'type': ['null', 'string', 'integer']},
            # To test flattening a deeply nested property
            'custom_fields': {
                'properties': {
                    'app': {
                        'properties': {
                            'value': {
                                'type': [
                                    'null',
                                    'string'
                                ]
                            }
                        },
                        'type': [
                            'null',
                            'object'
                        ]
                    }
                },
                'type': [
                    'null',
                    'object'
                ]
            },
            'other': {'type': ['null', 'string']}
        },
        'type': ['null', 'object'],
        'additionalProperties': True
    },
    'key_properties': ['id']
}

@pytest.fixture(scope='module')
def dbsync_class():
    return DbSync({'schema': 'test_schema'}, BASE_SCHEMA)
 
@pytest.mark.parametrize(
    'prop,expected',
    [
        ({'type': ['object'], 'format': 'date-time'}, 'jsonb'),
        ({'type': ['array']}, 'jsonb'),
        ({'type': ['object', 'array'], 'format': 'date-time'}, 'jsonb'),
        ({'type': ['string'], 'format': 'date-time'}, 'timestamp with time zone'),
        ({'type': ['boolean', 'integer', 'number'], 'format': 'date'}, 'date'),
        ({'type': ['boolean', 'integer', 'number']}, 'numeric'),
        ({'type': ['integer', 'string']}, 'character varying'),
        ({'type': ['boolean', 'integer']}, 'boolean'),
        ({'type': ['integer']}, 'bigint'),
        ({'type': ['string']}, 'character varying'),
    ]
)
def test_column_type(prop: dict, expected: str):
    assert column_type(prop) == expected

@pytest.mark.parametrize(
    'additional_prop_kwargs,expected',
    [
        (
            # Test field with anyOf property
            {
                'f': {
                    'anyOf': [
                        {'type': ['null', 'string'], 'format': 'date-time'},
                        {'type': ['null', 'string']}
                    ]
                }
            },
            {
                'id': {'type': ['null', 'string', 'integer']},
                'custom_fields__app__value': {'type': ['null', 'string']},
                'other': {'type': ['null', 'string']},
                'f': {'type': ['null', 'string'], 'format': 'date-time'}
            }
        ),
        (
            # Test field with array type, ensure 'items'
            {
                'f': {'type': ['array'], 'items': ['string']}
            },
            {
                'id': {'type': ['null', 'string', 'integer']},
                'custom_fields__app__value': {'type': ['null', 'string']},
                'other': {'type': ['null', 'string']},
                'f': {'type': ['array'], 'items': ['string']}
            }
        ),
        (
            # Test empty field
            {
                'f': {}
            },
            {
                'id': {'type': ['null', 'string', 'integer']},
                'custom_fields__app__value': {'type': ['null', 'string']},
                'other': {'type': ['null', 'string']},
            }
        ),
        (
            # Test overwriting existing schema field
            {
                'other': {'type': ['null', 'string', 'integer']},
            },
            {
                'id': {'type': ['null', 'string', 'integer']},
                'custom_fields__app__value': {'type': ['null', 'string']},
                'other': {'type': ['null', 'string', 'integer']},
            }
        ),
        (   
            # Test creating a new nested property
            {
                'deep_other': {
                    'properties': {
                        'value': {
                            'type': ['null', 'integer']
                        }
                    },
                    'type': ['null', 'object']
                },
            },
            {
                'id': {'type': ['null', 'string', 'integer']},
                'custom_fields__app__value': {'type': ['null', 'string']},
                'other': {'type': ['null', 'string']},
                'deep_other__value': {'type': ['null', 'integer']},
            }
        ),
    ]
)
def test_flatten_schema(additional_prop_kwargs: dict, expected: dict):
    schema = deepcopy(BASE_SCHEMA)['schema']
    schema['properties'].update(**additional_prop_kwargs)
    _sort = lambda item: item[0]
    sorted_flattened = sorted(flatten_schema(schema).items(), key=_sort)
    sorted_expected = sorted(expected.items(), key=_sort)
    assert sorted_flattened == sorted_expected

def test_flatten_record():
    record = {'id': '1', 'custom_fields': {'app': {'value': 'nested'}}}
    expected = {'id': '1', 'custom_fields__app__value': 'nested'}
    assert flatten_record(record) == expected

@pytest.mark.parametrize(
    'table_name,is_temporary,expected',
    [
        ('test', True, 'test_temp'),
        ('test', False, 'test_schema.test'),
        ('test', False, 'test_schema.test'),
        ('TestTable', True, 'test_table_temp'),
        ('Test_table', False, 'test_schema.test_table'),
        ('test Table', False, 'test_schema.test__table'),
    ]
)
def test_dbsync_table_name(table_name: str, is_temporary: bool, expected: str, dbsync_class: DbSync):
    assert dbsync_class.table_name(table_name, is_temporary) == expected

@pytest.mark.parametrize(
    'record,expected',
    [
        ({'id': '1', 'custom_fields': {'app': {'value': 'nested'}}}, ['1', 'nested', '']),
        ({'custom_fields': {'app': {'value': 'nested'}}}, ['', 'nested', '']),
        ({'other': 'some_other'}, ['', '', 'some_other']),
        ({'dne': 'dne'}, ['', '', ''])
    ]
)
def test_record_to_csv_row(record: dict, expected: list, dbsync_class: DbSync):
    assert sorted(dbsync_class.record_to_csv_row(record)) == sorted(expected)
