from collections import namedtuple
import pytest
from copy import deepcopy
from target_postgres.db_sync import DbSync, column_type, flatten_record, flatten_schema, most_general_type, JSONSCHEMA_TYPES

BASE_SCHEMA = {
    'type': 'SCHEMA',
    'stream': 'tests',
    'schema': {
        'properties': {
            'id': {'type': ['null', 'string', 'integer']},
            # To test not flattening a dictionary
            'dict_fields': {
                'type': [
                    'null',
                    'object'
                ]
            },
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
        ({'type': ['boolean', 'integer', 'number'], 'format': 'date'}, 'numeric'),
        ({'type': ['boolean', 'integer', 'number']}, 'numeric'),
        ({'type': ['integer', 'string']}, 'character varying'),
        ({'type': ['boolean', 'integer']}, 'bigint'),
        ({'type': ['integer']}, 'bigint'),
        ({'type': ['string']}, 'character varying'),
        ({'type': ['null', 'array', 'string'], 'items': {'type': ['null', 'string']}}, 'character varying'),

        # Ensure we don't get errors when we use an invalid type
        ({'type': ['NOT A REAL TYPE!']}, 'character varying'),
    ],
    ids=repr
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
                'dict_fields': {'type': ['null', 'object']},
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
                'dict_fields': {'type': ['null', 'object']},
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
                'dict_fields': {'type': ['null', 'object']},
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
                'dict_fields': {'type': ['null', 'object']},
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
                'dict_fields': {'type': ['null', 'object']},
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

@pytest.mark.parametrize(
    'record,expected',
    [
        (
            {'id': '1', 'custom_fields': {'app': 'value'}},
            {'id': '1', 'custom_fields': '{"app": "value"}', 'custom_fields__app': 'value'}
        ),
        (
            # Test a nested field and perserves other key values
            {'id': '1', 'custom_fields': {'app': {'value': 'nested'}}},
            {'id': '1', 'custom_fields': '{"app": {"value": "nested"}}', 'custom_fields__app': '{"value": "nested"}', 'custom_fields__app__value': 'nested'}
        ),
        (
            # Test a nested field with multiple key values
            {'custom_fields': {'app': {'value': 'nested', 'value2': 'nested2'}}},
            {'custom_fields': '{"app": {"value": "nested", "value2": "nested2"}}', 'custom_fields__app': '{"value": "nested", "value2": "nested2"}', 'custom_fields__app__value': 'nested', 'custom_fields__app__value2': 'nested2'}
        ),
        (
            # Test a nested field with array value of same types, calls json.dumps
            {'custom_fields': {'app': {'value': ['1', '2']}}},
            {'custom_fields': '{"app": {"value": ["1", "2"]}}', 'custom_fields__app': '{"value": ["1", "2"]}', 'custom_fields__app__value': '["1", "2"]'}
        ),
        (
            # Test a nested field with array value of varying types, calls json.dumps
            {'custom_fields': {'app': {'value': [1, '2', {'a': 3}]}}},
            {'custom_fields': '{"app": {"value": [1, "2", {"a": 3}]}}', 'custom_fields__app': '{"value": [1, "2", {"a": 3}]}', 'custom_fields__app__value': '[1, "2", {"a": 3}]'}
        ),
        (
            # Test a nested field with tuple value of same types
            {'custom_fields': {'app': {'value': (1, 2)}}},
            {'custom_fields': '{"app": {"value": [1, 2]}}', 'custom_fields__app': '{"value": [1, 2]}', 'custom_fields__app__value': (1, 2)}
        ),
        (
            # Test a nested field with tuple value of varying types
            {'custom_fields': {'app': {'value': (1, '2', {'a': 3})}}},
            {'custom_fields': '{"app": {"value": [1, "2", {"a": 3}]}}', 'custom_fields__app': '{"value": [1, "2", {"a": 3}]}', 'custom_fields__app__value': (1, '2', {'a': 3})}
        ),
        ({'id': 1}, {'id': 1}),
        (None, {}),
        ([], {}),
        ({}, {}),
        ('some_value', {}),
    ]
)
def test_flatten_record(record, expected: dict):
    actual = flatten_record(record)
    assert actual == expected
    assert isinstance(actual, dict)

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
        ({'id': '1', 'custom_fields': {'app': {'value': 'nested'}}}, ['1', 'nested', '', '']),
        ({'id': '1', 'dict_fields': {'app': {'value': 'nested'}}}, ['1', '', '', '{"app": {"value": "nested"}}']),
        ({'custom_fields': {'app': {'value': 'nested'}}}, ['', 'nested', '', '']),
        ({'other': 'some_other'}, ['', '', 'some_other', '']),
        ({'dne': 'dne'}, ['', '', '', ''])
    ]
)
def test_record_to_csv_row(record: dict, expected: list, dbsync_class: DbSync):
    assert sorted(dbsync_class.record_to_csv_row(record)) == sorted(expected)

@pytest.mark.parametrize(
    'record,key_props,expected',
    [   
        ({'id': '1', 'custom_fields': {'app': {'value': 'nested'}}}, [], None),
        ({}, ['id'], None),
        ({'id': '1', 'custom_fields': {'app': {'value': 'nested'}}}, ['id'], '1'),
        ({'test__primary': 1}, ['Test Primary'], '1'),
        ({'test__primary': 1, 'test_secondary': 2}, ['Test Primary', 'Test_secondary'], '1,2'),
    ]
)
def test_record_primary_key_string(record, key_props: list, expected: str, dbsync_class: DbSync):
    dbsync_class.stream_schema_message['key_properties'] = key_props
    assert dbsync_class.record_primary_key_string(record) == expected


@pytest.mark.parametrize(
    'types, expected',
    [
        # string always wins
        (JSONSCHEMA_TYPES, 'string'),  
        (('string', 'number'), 'string'),

        (('integer', 'number'), 'number'),
        (('boolean', 'integer', 'number'), 'number'),
        (('boolean', 'integer'), 'integer'),

        (('null', 'string'), 'string'),
        (('array', 'object'), 'object'),

        # None of these types generalize to each other, so we need to choose string as the general type
        (('array', 'null', 'boolean'), 'string'),
        # We don't know about these thypes, so again assume string is best
        (('fake type 1', 'fake type 2', 'fake type 3'), 'string'),
    ],
    ids=repr
)
def test_most_general_type(types, expected):
    assert most_general_type(types) == expected