#!/bin/env python

import json
import time as time_mod
import logging
from argparse import ArgumentParser

import config

logging.basicConfig(level=logging.WARNING)

# TODO create dict class with versioning instead of explicit access


def _get_sorted_versions(versions):
    return sorted(versions, key=lambda x: x[1])


def create_or_update(versions, val, time):
    # we assume sorted list here
    if len(versions) > 0 and versions[-1][0] == val:
        return versions
    versions.append([val, time])
    versions = _get_sorted_versions(versions)
    return versions


def dump_path(obj, path):
    with open(path, "w") as fp:
        json.dump(
            obj,
            fp,
            sort_keys=True,
            indent=4)


def load_path(path):
    with open(path, "r") as fp:
        return json.load(fp)


def process_items(nodes, db, mapping, time):
    for k, node_entry in nodes.items():
        versions = db.get(k, [])
        db[k] = create_or_update(versions, node_entry, time)


def run(nodes_file, db_file, time):
    nodes = load_path(nodes_file)
    try:
        db = load_path(db_file)
    except:
        db = {}
    process_items(nodes, db, config.KEY_MAPPING, time)
    dump_path(db, db_file)


if __name__ == "__main__":
    cur_time = int(time_mod.time())
    parser = ArgumentParser()
    parser.add_argument("input")
    parser.add_argument("output")
    parser.add_argument("--time", default=cur_time)
    args = parser.parse_args()

    run(args.input, args.output, args.time)
