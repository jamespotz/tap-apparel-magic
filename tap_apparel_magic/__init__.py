#!/usr/bin/env python3
'''Tap for ApparellMagic
TODO add detailed description
'''
import datetime
import itertools
import json
import os
import re
import sys
import time
import urllib

import backoff
import dateutil
import requests
import singer
import singer.metrics as metrics
from dateutil import parser
from requests.auth import HTTPBasicAuth
from singer import metadata, utils
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema

REQUIRED_CONFIG_KEYS = ["url", "token"]
LOGGER = singer.get_logger()

CONFIG = {
    "url": None,
    "token": None,
}


AUTH_WITH_PAGINATION = "token={0}&time={1}&pagination[page_size]=1000&"\
                       "pagination[page_number]={2}"


LAST_MODIFIED_QUERY = "&parameters[0][field]="\
    "last_modified_time&parameters[0][operator]=>=&" \
    "parameters[0][value]={3}"


ENDPOINTS = {
    "orders": f"/orders?{AUTH_WITH_PAGINATION}{LAST_MODIFIED_QUERY}",
    "products": f"/products?{AUTH_WITH_PAGINATION}{LAST_MODIFIED_QUERY}",
    "shipments": f"/shipments?{AUTH_WITH_PAGINATION}{LAST_MODIFIED_QUERY}",
    "customers": f"/customers?{AUTH_WITH_PAGINATION}{LAST_MODIFIED_QUERY}",
    "invoices": f"/invoices?{AUTH_WITH_PAGINATION}{LAST_MODIFIED_QUERY}",
    "vendors": f"/vendors?{AUTH_WITH_PAGINATION}&parameters[0][field]="
               "vendor_id&parameters[0][operator]=>=&"
               "parameters[0][value]={3}",
    "credit_memos": f"/credit_memos?{AUTH_WITH_PAGINATION}&parameters[0][field]="
                    "credit_memo_id&parameters[0][operator]=>=&"
                    "parameters[0][value]={3}",
    "purchase_orders": f"/purchase_orders?{AUTH_WITH_PAGINATION}{LAST_MODIFIED_QUERY}",
    "product_attributes": f"/product_attributes?{AUTH_WITH_PAGINATION}&parameters[0][field]="
                          "id&parameters[0][operator]=>=&"
                          "parameters[0][value]={3}",
    "size_ranges": f"/size_ranges?{AUTH_WITH_PAGINATION}&parameters[0][field]="
                   "id&parameters[0][operator]=>=&"
                   "parameters[0][value]={3}",
    "inventory": f"/inventory?{AUTH_WITH_PAGINATION}&parameters[0][field]="
                 "sku_id&parameters[0][operator]=>=&"
                 "parameters[0][value]={3}",
    "payments": f"/payments?{AUTH_WITH_PAGINATION}&parameters[0][field]="
                "payment_id&parameters[0][operator]=>=&"
                "parameters[0][value]={3}",

}


WITH_LAST_MODIFIED_TIME = [
    "orders",
    "shipments",
    "products",
    "customers",
    "invoices",
    "purchase_orders"
]

WITH_ID_ONLY = {
    "size_ranges",
    "product_attributes"
}


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    # Load schemas from schemas folder
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas


def discover():
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        stream_metadata = []
        key_properties = []
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=key_properties,
                metadata=stream_metadata,
                replication_key=None,
                is_view=None,
                database=None,
                table=None,
                row_count=None,
                stream_alias=None,
                replication_method=None,
            )
        )
    return Catalog(streams)


def get_endpoint(endpoint, kwargs):
    # Get the full url for the endpoint
    if endpoint not in ENDPOINTS:
        raise ValueError("Invalid endpoint {}".format(endpoint))

    token = kwargs[0]
    time_param = kwargs[1]
    page_number = kwargs[2]
    updated_time_or_id = kwargs[3]
    return CONFIG["url"]+ENDPOINTS[endpoint].format(
        token, time_param, page_number, updated_time_or_id)


def get_start(state, tap_stream_id, bookmark_key):
    current_bookmark = singer.get_bookmark(state, tap_stream_id, bookmark_key)
    if current_bookmark is None:
        # Records with last_modified_time
        if tap_stream_id in WITH_LAST_MODIFIED_TIME:
            return datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        # This are records without last_modified_time
        return 1

    if tap_stream_id not in WITH_LAST_MODIFIED_TIME:
        return 1

    return current_bookmark


def get_replication_key(stream_id, replication_key):
    # Get the bookmark column
    if replication_key is not None:
        return replication_key

    if stream_id in WITH_LAST_MODIFIED_TIME:
        return 'last_modified_time'

    # The current stream is using ID as primary key and does not have
    # last_modified_time
    if stream_id in WITH_ID_ONLY:
        return 'id'

    # The Inventory stream uses sku_id as its primary key
    if stream_id == 'inventory':
        return 'sku_id'

    # vendor_id and credit_memo_id
    return f'{stream_id[:-1]}_id'



def giveup(exc):
    return exc.response is not None \
        and 400 <= exc.response.status_code < 500 \
        and exc.response.status_code != 429


@utils.backoff((backoff.expo, requests.exceptions.RequestException), giveup)
@utils.ratelimit(20, 1)
def gen_request(stream_id, url):
    with metrics.http_request_timer(stream_id) as timer:
        resp = requests.get(url)
        timer.tags[metrics.Tag.http_status_code] = resp.status_code
        resp.raise_for_status()
        return resp.json()


def sync(state, catalog):
    """ Sync data from tap source """
    # Loop over selected streams in catalog
    counts = {}
    start_time = time.process_time()
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream: %s", stream.tap_stream_id)
        counts[stream.tap_stream_id] = 0

        singer.write_schema(stream.tap_stream_id,
                            stream.schema.to_dict(), stream.key_properties)

        bookmark_column = get_replication_key(stream.tap_stream_id, stream.replication_key)
        start = get_start(state, stream.tap_stream_id, bookmark_column)
        last_update = start
        page_number = 1
        with metrics.record_counter(stream.tap_stream_id) as counter:
            while True:
                endpoint = get_endpoint(stream.tap_stream_id, [
                    CONFIG["token"],
                    int(time.time()),
                    page_number,
                    start
                ])

                LOGGER.info("GET: %s", endpoint)
                tap_data = gen_request(stream.tap_stream_id, endpoint)
                total_pages = tap_data["meta"]["pagination"]["total_pages"]

                for row in tap_data["response"]:
                    counter.increment()
                    counts[stream.tap_stream_id] += 1
                    # write one or more rows to the stream:
                    singer.write_records(stream.tap_stream_id, [row])

                    if "last_modified_time" in row:
                        new_update_time = parser.parse(row["last_modified_time"])
                        old_update_time = parser.parse(last_update)
                        if new_update_time > old_update_time:
                            last_update = row["last_modified_time"]
                    else:
                        last_update = row[bookmark_column]

                if page_number >= int(total_pages):
                    # Weve reach the end of the page
                    break
                page_number += 1

        # update bookmark to the latest value
        singer.write_state({stream.tap_stream_id: {
            bookmark_column: last_update
        }})

        LOGGER.info('----------------------')
        for stream_id, stream_count in counts.items():
            LOGGER.info('%s: %d', stream_id, stream_count)
        LOGGER.info('----------------------')

    elapsed = time.process_time() - start_time
    LOGGER.info('Process time: %s', elapsed)


@utils.handle_top_exception(LOGGER)
def main():
    """ Entry point """
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    CONFIG.update(args.config)

    if args.discover:
        catalog = discover()
        catalog.dump()
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        sync(args.state, catalog)


if __name__ == "__main__":
    main()
