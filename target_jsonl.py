#!/usr/bin/env python3

import argparse
import io
import jsonschema
import obstore as obs
import os
import simplejson as json
import singer
import sys
from adjust_precision_for_schema import adjust_decimal_precision_for_schema
from datetime import datetime
from jsonschema import Draft4Validator, FormatChecker
from obstore.store import S3Store
from pathlib import Path

logger = singer.get_logger()


def _is_s3_path(path: str) -> bool:
    return isinstance(path, str) and path.startswith("s3://")


def _parse_s3_uri(path: str):
    """Return (bucket, key_prefix) from an s3:// URI."""
    without_scheme = path[5:]  # drop 's3://'
    slash = without_scheme.find("/")
    if slash == -1:
        return without_scheme, ""
    return without_scheme[:slash], without_scheme[slash + 1:].rstrip("/")


def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()



def persist_messages(
    messages,
    destination_path,
    custom_name=None,
    do_timestamp_file=True
):
    state = None
    schemas = {}
    key_properties = {}
    validators = {}

    use_s3 = _is_s3_path(destination_path)
    s3_buffers: dict = {}   # stream -> io.StringIO
    s3_filenames: dict = {}  # stream -> (bucket, key)

    timestamp_file_part = '-' + datetime.now().strftime('%Y%m%dT%H%M%S') if do_timestamp_file else ''

    for message in messages:
        try:
            o = singer.parse_message(message).asdict()
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(message))
            raise
        message_type = o['type']
        if message_type == 'RECORD':
            if o['stream'] not in schemas:
                raise Exception(
                    "A record for stream {}"
                    "was encountered before a corresponding schema".format(o['stream'])
                )

            try: 
                validators[o['stream']].validate((o['record']))
            except jsonschema.ValidationError as e:
                logger.error(f"Failed parsing the json schema for stream: {o['stream']}.")
                raise e

            filename = (custom_name or o['stream']) + timestamp_file_part + '.jsonl'

            if use_s3:
                if o['stream'] not in s3_buffers:
                    s3_buffers[o['stream']] = io.StringIO()
                    bucket, prefix = _parse_s3_uri(destination_path)
                    key = f"{prefix}/{filename}" if prefix else filename
                    s3_filenames[o['stream']] = (bucket, key)
                s3_buffers[o['stream']].write(json.dumps(o['record']) + '\n')
            else:
                if destination_path:
                    Path(destination_path).mkdir(parents=True, exist_ok=True)
                filepath = os.path.expanduser(os.path.join(destination_path, filename))
                with open(filepath, 'a', encoding='utf-8') as json_file:
                    json_file.write(json.dumps(o['record']) + '\n')

            state = None
        elif message_type == 'STATE':
            logger.debug('Setting state to {}'.format(o['value']))
            state = o['value']
        elif message_type == 'SCHEMA':
            stream = o['stream']
            schemas[stream] = o['schema']
            adjust_decimal_precision_for_schema(schemas[stream])
            validators[stream] = Draft4Validator((o['schema']))
            key_properties[stream] = o['key_properties']
        else:
            logger.warning("Unknown message type {} in message {}".format(o['type'], o))

    if use_s3 and s3_buffers:
        for stream, buf in s3_buffers.items():
            bucket, key = s3_filenames[stream]
            store = S3Store(bucket)
            obs.put(store, key, buf.getvalue().encode("utf-8"))
            logger.info(f"Wrote s3://{bucket}/{key}")

    return state


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file')
    args = parser.parse_args()

    if args.config:
        with open(args.config) as input_json:
            config = json.load(input_json)
    else:
        config = {}

    input_messages = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    state = persist_messages(
        input_messages,
        config.get('destination_path', ''),
        config.get('custom_name', ''),
        config.get('do_timestamp_file', True)
    )

    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()
