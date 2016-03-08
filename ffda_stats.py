#!/usr/bin/env python
import logging
import pprint
import socket
import time
from contextlib import contextmanager

import requests

logger = logging.getLogger(__name__)


@contextmanager
def get_socket(host, port):
    sock = socket.socket()
    sock.settimeout(1)
    sock.connect((host, port))
    yield sock
    sock.close()


def write_to_graphite(data, prefix='freifunk', log=None):
    # {u'status': u'up', u'graph': {u'max': 539, u'uptime': 90262, u'total': 9435, u'connected': 297, u'cap': 3000}, u'timestamp': 1421072166316}
    now = time.time()
    with get_socket('127.0.0.1', 2013) as s:
        for key, value in data.items():
            line = "%s.%s %s %s\n" % (prefix, key, value, now)
            #            if not log is None:
            #                if 'andi-' in key:
            #                    log.debug(line)
            s.sendall(line.encode('latin-1'))


def parse_graph(gateways):
    # parse graph
    URL = 'https://map.darmstadt.freifunk.net/data.graph.json'
    update = {}

    if len(gateways) > 0:
        data = requests.get(URL, timeout=1).json()


def main():
    logging.basicConfig(level=logging.DEBUG)
    while True:
        pprinter = pprint.PrettyPrinter(indent=4)

        URL = 'https://map.darmstadt.freifunk.net/data/nodes.json'

        gateways = []

        try:
            client_count = 0

            data = requests.get(URL, timeout=1).json()
            nodes = data['nodes']
            known_nodes = len(nodes.keys())
            online_nodes = 0
            update = {}
            gateway_count = 0
            for node_mac, node in nodes.items():
                try:
                    hostname = node['nodeinfo']['hostname']

                    flags = node['flags']
                    if flags['online']:
                        online_nodes += 1

                    if flags['gateway']:
                        gateway_count += 1
                        gateways.append(hostname)

                    statistics = node['statistics']
                    # try:
                    #  loadavg = statistics['loadavg']
                    #  update['%s.loadavg' % hostname] = loadavg
                    # except KeyError:
                    #  pass
                    # try:
                    #  uptime = statistics['uptime']
                    #  update['%s.uptime' % hostname] = uptime
                    # except KeyError:
                    #  pass

                    try:
                        clients = statistics['clients']
                        client_count += int(clients)
                        # update['%s.clients' % hostname] = clients
                    except KeyError:
                        pass

                    try:
                        traffic = statistics['traffic']
                        for key in ['tx', 'rx', 'mgmt_tx', 'mgmt_rx', 'forward']:
                            update['%s.traffic.%s.packets' % (hostname, key)] = traffic[key]['packets']
                            update['%s.traffic.%s.bytes' % (hostname, key)] = traffic[key]['bytes']
                    except KeyError:
                        pass

                    try:
                        key = 'firmware.release.%s' % node['nodeinfo']['software']['firmware']['release']
                        if key not in update:
                            update[key] = 0
                        update[key] += 1
                    except KeyError:
                        pass

                    try:
                        key = 'firmware.base.%s' % node['nodeinfo']['software']['firmware']['base']
                        if key not in update:
                            update[key] = 0
                        update[key] += 1
                    except KeyError:
                        pass

                    for key in ['memory_usage', 'rootfs_usage', 'uptime', 'loadavg', 'clients']:
                        try:
                            val = statistics[key]
                            update['%s.%s' % (hostname, key)] = val
                        except KeyError:
                            pass
                except KeyError as e:
                    print(time.time())
                    print('error while reading ', node_mac)
                    print(e)

                #            print(time.time())
            update['clients'] = client_count
            update['known_nodes'] = known_nodes
            update['online_nodes'] = online_nodes
            update['gateways'] = gateway_count
            #            print(client_count)
            #pprint.pprint(update)
            write_to_graphite(update, log=logger)
        except Exception as e:
            print(e)

        time.sleep(25)


if __name__ == "__main__":
    main()
