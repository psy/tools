#!/usr/bin/env python3
import json
import requests
import re
import itertools
from contextlib import contextmanager
import pprint
import time
import socket
import logging

logger = logging.getLogger(__name__)


def pairwise(iterable):
    "s -> (s0,s1), (s2,s3), (s4, s5), ..."
    a = iter(iterable)
    return zip(a, a)


@contextmanager
def get_socket(host, port):
    sock = socket.socket()
    sock.settimeout(1)
    sock.connect((host, port))
    yield sock
    sock.close()


def write_to_graphite(data, prefix='freifunk', hostname=socket.gethostname()):
    if '.' in hostname:
        hostname = hostname.split('.')[0]
    now = time.time()
    with get_socket('stats.darmstadt.freifunk.net', 2013) as s:
        for key, value in data.items():
            line = "%s.%s.%s %s %s\n" % (prefix, hostname, key, value, now)
            s.sendall(line.encode('latin-1'))

def main():
    device_name_mapping = {
        'freifunk': 'ffda-br',
        'bat0': 'ffda-bat',
        'mesh-vpn': 'ffda-vpn'
    }
    device_whitelist = [
        'eth0',
        'tun-ffrl-ber',
        'tun-ffrl-dus',
        'tun-ffrl-fra',
        'tun-ffda-gw01',
        'tun-ffda-gw02',
        'tun-ffda-gw03',
        'tun-ffda-gw04',
        'ffda-vpn',
        'ffda-bat',
        'ffda-br',
        'icvpn',
        'ffda-transport'
    ]

    fields = [
        'bytes', 'packets', 'errs', 'drop', 'fifo',
        'frame', 'compressed', 'multicast',
    ]
    field_format = '(?P<{direction}_{field}>\d+)'
    
    pattern = re.compile(
        '^\s*(?P<device_name>[\w-]+):\s+' + '\s+'.join(
            itertools.chain.from_iterable((field_format.format(direction=direction, field=field)
                                           for field in fields) for direction in ['rx', 'tx'])
        )
    )

    update = {}
    with open('/proc/net/dev') as fh:
        lines = fh.readlines()
        for line in lines:
            m = pattern.match(line)
            if m:
                groupdict = m.groupdict()
                device_name = groupdict.pop('device_name')
                device_name = device_name_mapping.get(device_name, device_name)
                if device_name in device_whitelist or device_name.endswith('-vpn') or \
                        device_name.endswith('-bat') or \
                        device_name.endswith('-br') or \
                        device_name.endswith('-transport'):
                    for key, value in groupdict.items():
                        direction, metric = key.split('_')
                        update['%s.%s.%s' % (device_name, direction, metric)] = value

    with open('/proc/loadavg', 'r') as fh:
        line = fh.read()
        values = line.split(' ', 3)
        update['load.15'] = values[0]
        update['load.5'] = values[1]
        update['load.1'] = values[2]

    for key in  ['count', 'max']:
        try:
            with open('/proc/sys/net/netfilter/nf_conntrack_%s' % key, 'r') as fh:
                update['netfilter.%s' % key] = fh.read().strip()
        except IOError as e:
            pass

    with open('/proc/net/snmp6', 'r') as fh:
        for line in fh.readlines():
            key, value = line.split(' ', 1)
            value = value.strip()
            update['ipv6.%s' % key] = value

    with open('/proc/net/snmp', 'r') as fh:
        for heading, values in pairwise(fh.readlines()):
            section, headings = heading.split(':')
            headings = headings.strip().split(' ')
            _, values = values.split(':')
            values = values.strip().split(' ')
            for key, value in zip(headings, values):
                update['ipv4.%s.%s' % (section, key)] = value

    with open('/proc/stat', 'r') as fh:
        for line in fh.readlines():
            key, value = line.split(' ', 1)
            if key == 'ctxt':
                update['context_switches'] = value.strip()
                break


    #pprint.pprint(update)
    write_to_graphite(update)


if __name__ == "__main__":
    main()
