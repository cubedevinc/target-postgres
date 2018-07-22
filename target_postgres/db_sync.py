import json
import psycopg2
import psycopg2.extras
import singer

logger = singer.get_logger()


def column_type(schema_property):
    property_type = schema_property['type']
    property_format = schema_property['format'] if 'format' in schema_property else None
    if property_format == 'date-time':
        return 'timestamp'
    elif 'number' in property_type:
        return 'decimal'
    elif 'integer' in property_type:
        return 'integer'
    elif 'boolean' in property_type:
        return 'bool'
    else:
        return 'varchar'


def column_attributes(name, key_properties):
    if name in key_properties:
        return ' PRIMARY KEY'
    else:
        return ''


def column_clause(name, schema_property, key_properties):
    return '{} {}{}'.format(name, column_type(schema_property), column_attributes(name, key_properties))


class DbSync:
    def __init__(self, connection_config):
        self.connection_config = connection_config
        self.schema_name = self.connection_config['schema']

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

    def record_to_csv_line(self, record, stream_schema_message):
        return ','.join(
            [
                json.dumps(record[name]) if name in record else ''
                for name in stream_schema_message['schema']['properties']
            ]
        )

    def load_csv(self, stream_schema_message, file, count):
        file.seek(0)
        stream = stream_schema_message['stream']
        logger.info("Loading {} rows into '{}'".format(count, stream))

        with self.open_connection() as connection:
            with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(self.create_table_query(stream_schema_message, True))
                cur.copy_expert("COPY {} FROM STDIN WITH CSV".format(self.table_name(stream, True)), file)
                cur.execute(self.update_from_temp_table(stream_schema_message))
                logger.info(cur.statusmessage)
                cur.execute(self.insert_from_temp_table(stream_schema_message))
                logger.info(cur.statusmessage)
                cur.execute(self.drop_temp_table(stream_schema_message))

    def insert_from_temp_table(self, stream_schema_message):
        columns = self.column_names(stream_schema_message)
        table = self.table_name(stream_schema_message['stream'], False)
        temp_table = self.table_name(stream_schema_message['stream'], True)
        return """INSERT INTO {} ({}) 
        (SELECT s.* FROM {} s LEFT OUTER JOIN {} t ON s.id = t.id WHERE t.id is null)
        """.format(table, ', '.join(columns), temp_table, table)

    def update_from_temp_table(self, stream_schema_message):
        columns = self.column_names(stream_schema_message)
        table = self.table_name(stream_schema_message['stream'], False)
        temp_table = self.table_name(stream_schema_message['stream'], True)
        return """UPDATE {} SET {} FROM {} s 
        WHERE s.id = {}.id
        """.format(table, ', '.join(['{}=s.{}'.format(c, c) for c in columns]), temp_table, table)

    def drop_temp_table(self, stream_schema_message):
        temp_table = self.table_name(stream_schema_message['stream'], True)
        return "DROP TABLE {}".format(temp_table)

    def column_names(self, stream_schema_message):
        return [name for name in stream_schema_message['schema']['properties']]

    def create_table_query(self, stream_schema_message, is_temporary=False):
        columns = [
            column_clause(
                name,
                stream_schema_message['schema']['properties'][name],
                stream_schema_message['key_properties']
            )
            for name in stream_schema_message['schema']['properties']
        ]

        return 'CREATE {}TABLE {} ({})'.format(
            'TEMP ' if is_temporary else '',
            self.table_name(stream_schema_message['stream'], is_temporary),
            ', '.join(columns)
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

    def update_columns(self, stream_schema_message):
        stream = stream_schema_message['stream']
        columns = self.get_table_columns(stream)
        columns_dict = {column['column_name'].lower(): column for column in columns}

        columns_to_add = [
            column_clause(
                name,
                properties_schema,
                stream_schema_message['key_properties']
            )
            for (name, properties_schema) in stream_schema_message['schema']['properties'].items()
            if not columns_dict[name.lower()]
        ]

        for column in columns_to_add:
            add_column = "ALTER TABLE {} ADD COLUMN {}".format(self.table_name(stream, False), column)
            logger.info('Adding column: {}'.format(add_column))
            self.query(add_column)

    def sync_table(self, stream_schema_message):
        stream = stream_schema_message['stream']
        found_tables = [table for table in (self.get_tables()) if table['table_name'].lower() == stream.lower()]
        if len(found_tables) == 0:
            query = self.create_table_query(stream_schema_message)
            logger.info("Table '{}' does not exist. Creating... {}".format(stream, query))
            self.query(query)
        else:
            logger.info("Table '{}' exists".format(stream))
            self.update_columns(stream_schema_message)

