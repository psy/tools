#!/bin/env python

import sys
import json
import datetime
from bson import json_util
from functools import reduce

dt = datetime.datetime
argv = sys.argv

KEY_MAPPING = {
    'node_id': 'node_id',
    'contact': 'owner.contact',
    'hostname': 'hostname'
}


def get_recursive(path, p_dict):
    return reduce(dict.__getitem__, path.split("."), p_dict)


# TODO create dict class with versioning instead of explicit access
def create_or_update(target, src, target_key, src_key):
    versions = target.get(target_key, [])

    try:
        val = get_recursive(src_key, src)
    except KeyError:
        return
    else:
        target[target_key] = make_entry(versions, val)


def make_entry(versions, val):
    # we assume sorted list here
    if len(versions) > 0 and versions[-1][0] == val:
            return versions
    versions.append([val, dt.utcnow()])
    return versions


def get_current(versioned_dict, key):
    return sorted(versioned_dict[key].items(), key=lambda x: x[1])[0]


def dump_path(obj, path):
    with open(path, "w") as fp:
        json.dump(
            obj,
            fp,
            default=json_util.default,
            sort_keys=True,
            indent=4)


def load_path(path):
    with open(path, "r") as fp:
        return json.load(fp, object_hook=json_util.object_hook)


def process_items(nodes, db, mapping):
    for k, node_entry in nodes.items():
        db_entry = db.get(k, {})
        for target_key, src_key in KEY_MAPPING.items():
            create_or_update(db_entry, node_entry, target_key, src_key)
        db[k] = db_entry


def run(nodes_file, db_file):
    nodes = load_path(nodes_file)
    try:
        db = load_path(db_file)
    except:
        db = {}
    process_items(nodes, db, KEY_MAPPING)
    dump_path(db, db_file)


if __name__ == "__main__":
    run(argv[1], argv[2])
