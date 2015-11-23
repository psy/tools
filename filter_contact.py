#!/bin/env python

import sys
import json
import datetime
from bson import json_util
from hashlib import md5 as hash_func


FILTER_FIELDS = ['node_id', 'owner', 'hostname']
dt = datetime.datetime


def stable_hash(entry):
    return hash_func(entry.encode('ascii')).hexdigest()


# TODO create dict class with versioning instead of explicit access
def create_or_update(ex, item, field):
    k, v = item
    try:
        entry = v[field]
    except KeyError:
        return

    try:
        tmp_mac = ex[k]
    except KeyError:
        tmp_mac = {}
        ex[k] = tmp_mac

    try:
        tmp = tmp_mac[field]
    except KeyError:
        tmp = {}
        tmp_mac[field] = tmp

    if isinstance(entry, dict):
        entry_hash = stable_hash(str(sorted(entry.items())))
    else:
        entry_hash = stable_hash(entry)
    if entry_hash not in tmp and field in v:
        tmp[entry_hash] = {
            "time": dt.utcnow(),
            "val": entry}


with open(sys.argv[2], "r+") as db:
    ex = json.load(db, object_hook=json_util.object_hook)

with open(sys.argv[1]) as f:
    nodes = json.load(f)
    for item in nodes.items():
        for field in FILTER_FIELDS:
            create_or_update(ex, item, field)

with open(sys.argv[2], "w") as db:
    json.dump(ex, db, default=json_util.default, sort_keys=True, indent=4)
