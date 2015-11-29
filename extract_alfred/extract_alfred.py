#!/bin/env python

import json
import time as time_mod
import logging
from argparse import ArgumentParser
from functools import reduce

import config

logging.basicConfig(level=logging.WARNING)


def get_recursive(path, p_dict):
    logging.debug("Getting path %s from obj %s", path, p_dict)
    return reduce(dict.__getitem__, path.split("."), p_dict)


# TODO create dict class with versioning instead of explicit access
def create_or_update(target, src, target_key, src_key, time):
    versions = target.get(target_key, [])

    try:
        val = get_recursive(src_key, src)
    # it's not an error if key does not exists
    # TODO treat it individually for different keys
    except KeyError:
        return
    #also handle TypeError as some nodes have non-expected types here
    except TypeError as e:
        logging.warning(repr(e))
    else:
        target[target_key] = make_entry(versions, val, time)


def make_entry(versions, val, time):
    # we assume sorted list here
    if len(versions) > 0 and versions[-1][0] == val:
        return versions
    versions.append([val, time])
    return versions


def get_current(versioned_dict, key):
    return sorted(versioned_dict[key].items(), key=lambda x: x[1])[0]


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
        db_entry = db.get(k, {})
        for target_key, src_key in mapping.items():
            create_or_update(db_entry, node_entry, target_key, src_key, time)
        db[k] = db_entry


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
