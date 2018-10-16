#!/usr/bin/env python3

import argparse
import io
import os
import sys
import json
import threading
import http.client
import urllib
from datetime import datetime
import collections
from tempfile import TemporaryFile

import pkg_resources
from jsonschema.validators import Draft4Validator
import singer
from target_postgres.db_sync import DbSync

logger = singer.get_logger()


def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()


def persist_lines(config, lines):
    state = None
    schemas = {}
    key_properties = {}
    headers = {}
    validators = {}
    records_to_load = {}
    csv_files_to_load = {}
    row_count = {}
    stream_to_sync = {}
    batch_size = config['batch_size'] if 'batch_size' in config else 100000

    now = datetime.now().strftime('%Y%m%dT%H%M%S')

    # Loop over lines from stdin
    for line in lines:
        try:
            o = json.loads(line)
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(line))
            raise

        if 'type' not in o:
            raise Exception("Line is missing required key 'type': {}".format(line))
        t = o['type']

        if t == 'RECORD':
            if 'stream' not in o:
                raise Exception("Line is missing required key 'stream': {}".format(line))
            if o['stream'] not in schemas:
                raise Exception(
                    "A record for stream {} was encountered before a corresponding schema".format(o['stream']))

            # Get schema for this record's stream
            stream = o['stream']

            # Validate record
            validators[stream].validate(o['record'])

            primary_key_string = stream_to_sync[stream].record_primary_key_string(o['record'])
            if not primary_key_string:
                primary_key_string = 'RID-{}'.format(row_count[stream])

            if stream not in records_to_load:
                records_to_load[stream] = {}

            records_to_load[stream][primary_key_string] = o['record']
            row_count[stream] = len(records_to_load[stream])

            if row_count[stream] >= batch_size:
                 flush_records(stream, records_to_load, row_count, stream_to_sync)

            state = None
        elif t == 'STATE':
            logger.debug('Setting state to {}'.format(o['value']))
            state = o['value']
        elif t == 'SCHEMA':
            if 'stream' not in o:
                raise Exception("Line is missing required key 'stream': {}".format(line))
            stream = o['stream']
            schemas[stream] = o
            validators[stream] = Draft4Validator(o['schema'])
            if 'key_properties' not in o:
                raise Exception("key_properties field is required")
            key_properties[stream] = o['key_properties']
            stream_to_sync[stream] = DbSync(config, o)
            stream_to_sync[stream].create_schema_if_not_exists()
            stream_to_sync[stream].sync_table()
            row_count[stream] = 0
            csv_files_to_load[stream] = TemporaryFile(mode='w+b')
        elif t == 'ACTIVATE_VERSION':
            logger.debug('ACTIVATE_VERSION message')
        else:
            raise Exception("Unknown message type {} in message {}"
                            .format(o['type'], o))

    for (stream, count) in row_count.items():
        if count > 0:
            flush_records(stream, records_to_load, row_count, stream_to_sync)

    return state


def flush_records(stream, records_to_load, row_count, stream_to_sync):
    sync = stream_to_sync[stream]
    csv_file = TemporaryFile(mode='w+b')

    for record in records_to_load[stream].values():
        csv_line = sync.record_to_csv_line(record)
        csv_file.write(bytes(csv_line + '\n', 'UTF-8'))

    sync.load_csv(csv_file, row_count[stream])
    row_count[stream] = 0
    records_to_load[stream] = {}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file')
    args = parser.parse_args()

    if args.config:
        with open(args.config) as input:
            config = json.load(input)
    else:
        config = {}

    input = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    state = persist_lines(config, input)

    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()
