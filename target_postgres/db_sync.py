import json
import psycopg2
import psycopg2.extras
import singer
import collections
import inflection
import re
import itertools

logger = singer.get_logger()


def column_type(schema_property):
    property_type = schema_property['type']
    property_format = schema_property['format'] if 'format' in schema_property else None
    if 'object' in property_type or 'array' in property_type:
        return 'JSONB'
    elif property_format == 'date-time':
        return 'timestamp'
    elif 'number' in property_type:
        return 'decimal'
    elif 'integer' in property_type:
        return 'bigint'
    elif 'boolean' in property_type:
        return 'bool'
    else:
        return 'varchar'


def inflect_column_name(name):
    new_name = inflection.underscore(name)
    return new_name\
        .replace('properties', 'props')\
        .replace('timestamp', 'ts')\
        .replace('date', 'dt')\
        .replace('from', 'from_col')\
        .replace('associated', 'assoc')


def column_clause(name, schema_property):
    return '{} {}'.format(name, column_type(schema_property))


def flatten_key(k, parent_key, sep):
    if len(parent_key + k) > 40:
        reduced_key = re.sub(r'[a-z]', '', inflection.camelize(k))
        k = reduced_key if len(reduced_key) > 1 else k[0:3]
    return parent_key + sep + inflect_column_name(k) if parent_key else inflect_column_name(k)


def flatten_schema(d, parent_key='', sep='__'):
    items = []
    for k, v in d['properties'].items():
        new_key = flatten_key(k, parent_key, sep)
        if 'object' in v['type']:
            items.extend(flatten_schema(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))

    key_func = lambda item: item[0]
    sorted_items = sorted(items, key=key_func)
    for k, g in itertools.groupby(sorted_items, key=key_func):
        if len(list(g)) > 1:
            raise ValueError('Duplicate column name produced in schema: {}'.format(k))

    return dict(sorted_items)


def flatten_record(d, parent_key='', sep='__'):
    items = []
    for k, v in d.items():
        new_key = flatten_key(k, parent_key, sep)
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten_record(v, new_key, sep=sep).items())
        else:
            items.append((new_key, json.dumps(v) if type(v) is list else v))
    return dict(items)


def primary_column_names(stream_schema_message):
    return [inflect_column_name(p) for p in stream_schema_message['key_properties']]


class DbSync:
    def __init__(self, connection_config, stream_schema_message):
        self.connection_config = connection_config
        self.schema_name = self.connection_config['schema']
        self.stream_schema_message = stream_schema_message
        self.flatten_schema = flatten_schema(stream_schema_message['schema'])

    def open_connection(self):
        conn_string = "host='{}' dbname='{}' user='{}' password='{}' port='{}'".format(
            self.connection_config['host'],
            self.connection_config['dbname'],
            self.connection_config['user'],
            self.connection_config['password'],
            self.connection_config['port']
        )

        return psycopg2.connect(conn_string)

    def query(self, query, params=None):
        with self.open_connection() as connection:
            with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(
                    query,
                    params
                )

                if cur.rowcount > 0:
                    return cur.fetchall()
                else:
                    return []

    def copy_from(self, file, table):
        with self.open_connection() as connection:
            with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.copy_from(file, table)

    def table_name(self, table_name, is_temporary):
        if is_temporary:
            return '{}_temp'.format(table_name)
        else:
            return '{}.{}'.format(self.schema_name, table_name)

    def record_to_csv_line(self, record):
        flatten = flatten_record(record)
        return ','.join(
            [
                json.dumps(flatten[name]) if name in flatten and flatten[name] else ''
                for name in self.flatten_schema
            ]
        )

    def load_csv(self, file, count):
        file.seek(0)
        stream_schema_message = self.stream_schema_message
        stream = stream_schema_message['stream']
        logger.info("Loading {} rows into '{}'".format(count, stream))

        with self.open_connection() as connection:
            with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(self.create_table_query(True))
                copy_sql = "COPY {} ({}) FROM STDIN WITH (FORMAT CSV, ESCAPE '\\')".format(
                    self.table_name(stream, True),
                    ', '.join(self.column_names())
                )
                logger.info(copy_sql)
                cur.copy_expert(
                    copy_sql,
                    file
                )
                cur.execute(self.update_from_temp_table())
                logger.info(cur.statusmessage)
                cur.execute(self.insert_from_temp_table())
                logger.info(cur.statusmessage)
                cur.execute(self.drop_temp_table())

    def insert_from_temp_table(self):
        stream_schema_message = self.stream_schema_message
        columns = self.column_names()
        table = self.table_name(stream_schema_message['stream'], False)
        temp_table = self.table_name(stream_schema_message['stream'], True)
        return """INSERT INTO {} ({}) 
        (SELECT s.* FROM {} s LEFT OUTER JOIN {} t ON {} WHERE {})
        """.format(
            table,
            ', '.join(columns),
            temp_table,
            table,
            self.primary_key_condition('t'),
            self.primary_key_null_condition('t')
        )

    def update_from_temp_table(self):
        stream_schema_message = self.stream_schema_message
        columns = self.column_names()
        table = self.table_name(stream_schema_message['stream'], False)
        temp_table = self.table_name(stream_schema_message['stream'], True)
        return """UPDATE {} SET {} FROM {} s 
        WHERE {}
        """.format(
            table,
            ', '.join(['{}=s.{}'.format(c, c) for c in columns]),
            temp_table,
            self.primary_key_condition(table)
        )

    def primary_key_condition(self, right_table):
        stream_schema_message = self.stream_schema_message
        names = primary_column_names(stream_schema_message)
        return ' AND '.join(['s.{} = {}.{}'.format(c, right_table, c) for c in names])

    def primary_key_null_condition(self, right_table):
        stream_schema_message = self.stream_schema_message
        names = primary_column_names(stream_schema_message)
        return ' AND '.join(['{}.{} is null'.format(right_table, c) for c in names])

    def drop_temp_table(self):
        stream_schema_message = self.stream_schema_message
        temp_table = self.table_name(stream_schema_message['stream'], True)
        return "DROP TABLE {}".format(temp_table)

    def column_names(self):
        return [name for name in self.flatten_schema]

    def create_table_query(self, is_temporary=False):
        stream_schema_message = self.stream_schema_message
        columns = [
            column_clause(
                name,
                schema
            )
            for (name, schema) in self.flatten_schema.items()
        ]

        primary_key = ["PRIMARY KEY ({})".format(', '.join(primary_column_names(stream_schema_message)))] \
            if len(stream_schema_message['key_properties']) else []

        return 'CREATE {}TABLE {} ({})'.format(
            'TEMP ' if is_temporary else '',
            self.table_name(stream_schema_message['stream'], is_temporary),
            ', '.join(columns + primary_key)
        )

    def create_schema_if_not_exists(self):
        schema_name = self.connection_config['schema']
        schema_rows = self.query(
            'SELECT schema_name FROM information_schema.schemata WHERE schema_name = %s',
            (schema_name,)
        )

        if len(schema_rows) == 0:
            self.query("CREATE SCHEMA IF NOT EXISTS {}".format(schema_name))

    def get_tables(self):
        return self.query(
            'SELECT table_name FROM information_schema.tables WHERE table_schema = %s',
            (self.schema_name,)
        )

    def get_table_columns(self, table_name):
        return self.query("""SELECT column_name, data_type 
      FROM information_schema.columns 
      WHERE table_name = %s AND table_schema = %s""", (table_name, self.schema_name))

    def update_columns(self):
        stream_schema_message = self.stream_schema_message
        stream = stream_schema_message['stream']
        columns = self.get_table_columns(stream)
        columns_dict = {column['column_name'].lower(): column for column in columns}

        columns_to_add = [
            column_clause(
                name,
                properties_schema
            )
            for (name, properties_schema) in self.flatten_schema.items()
            if name.lower() not in columns_dict
        ]

        for column in columns_to_add:
            add_column = "ALTER TABLE {} ADD COLUMN {}".format(self.table_name(stream, False), column)
            logger.info('Adding column: {}'.format(add_column))
            self.query(add_column)

    def sync_table(self):
        stream_schema_message = self.stream_schema_message
        stream = stream_schema_message['stream']
        found_tables = [table for table in (self.get_tables()) if table['table_name'].lower() == stream.lower()]
        if len(found_tables) == 0:
            query = self.create_table_query()
            logger.info("Table '{}' does not exist. Creating... {}".format(stream, query))
            self.query(query)
        else:
            logger.info("Table '{}' exists".format(stream))
            self.update_columns()

