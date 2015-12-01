#!/bin/env python

import json
import time as time_mod
import logging
from argparse import ArgumentParser
from hashlib import md5 as hash_func


# TODO use shelve, git or RDBMS for storage


def _get_sorted_versions(versions):
    return sorted(versions, key=lambda x: x[1])


def dict_hash(p_dict):
    hashee = json.dumps(p_dict, sort_keys=True)
    return hash_func(hashee.encode('ascii')).hexdigest()


def create_or_update(versions, val, time):
    entry_hash = dict_hash(val)
    if entry_hash not in versions:
        versions[entry_hash] = val
    return versions, entry_hash


def add_time(times, time, entry_hash):
    entry_times = times.get(entry_hash, [])
    entry_times.append(time)
    times[entry_hash] = entry_times


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


def process_items(nodes, db_nodes, db_times, time):
    for k, node_entry in nodes.items():
        versions = db_nodes.get(k, {})
        versions, entry_hash = create_or_update(versions, node_entry, time)
        db_nodes[k] = versions
        add_time(db_times, time, entry_hash)


def load_db(db_file):
    try:
        db = load_path(db_file)
    except:
        db = {}
    return db


def run(nodes_file, db_file, time):
    nodes = load_path(nodes_file)
    db_nodes_filename = db_file + ".json"
    db_times_filename = db_file + "-times.json"
    db_nodes = load_db(db_nodes_filename)
    db_times = load_db(db_times_filename)
    process_items(nodes, db_nodes, db_times, time)
    dump_path(db_nodes, db_nodes_filename)
    dump_path(db_times, db_times_filename)


if __name__ == "__main__":
    cur_time = int(time_mod.time())
    parser = ArgumentParser()
    parser.add_argument(
        "input",
        help="path to the 158.json",
        metavar="input_file")
    parser.add_argument(
        "output",
        help="prefix for the output files",
        metavar="output_prefix")
    parser.add_argument("--time", default=cur_time, type=int)
    parser.add_argument("--loglevel", default="WARNING")
    args = parser.parse_args()
    numeric_level = getattr(logging, args.loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s'.format(args.loglevel))
    logging.basicConfig(level=numeric_level)

    run(args.input, args.output, args.time)
