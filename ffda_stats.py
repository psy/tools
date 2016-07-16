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
    sock.settimeout(3)
    sock.connect((host, port))
    yield sock
    sock.close()


def write_to_graphite(data, prefix='freifunk', log=None):
    # {u'status': u'up', u'graph': {u'max': 539, u'uptime': 90262, u'total': 9435, u'connected': 297, u'cap': 3000}, u'timestamp': 1421072166316}
    now = time.time()
    with get_socket('graphite.h4ck.space', 2013) as s:
        for key, value in data.items():
            line = "%s.%s %s %s\n" % (prefix, key, value, now)
            #            if not log is None:
            #                if 'andi-' in key:
            #                    log.debug(line)
            s.sendall(line.encode('latin-1'))

def parse_graph(nodes):
    # parse graph
    URL = 'https://map.darmstadt.freifunk.net/data/graph.json'
    update = {}

    data = requests.get(URL, timeout=1).json()

    links = data.get('batadv', {}).get('links', [])
    graph_nodes = data.get('batadv', {}).get('nodes', [])

    del data

    edges = {}

    for link in links:
        key = '{}.{}'.format(min(link['source'], link['target']), max(link['source'], link['target']))
        if not key in edges:
            edges[key] = link

    del links

    deletes = []
    for key, edge in edges.items():
        try:
            source_id = graph_nodes[edge['source']]['node_id']
            target_id = graph_nodes[edge['target']]['node_id']
        except KeyError:
            deletes.append(key)
        else:
            try:
                edge['source'] = nodes[source_id]
                edge['target'] = nodes[target_id]
            except KeyError:
                pass


    for d in deletes:
        del edges[d]

    values = {}

    for key, edge in edges.items():
        try:
            key = 'link.{}.{}.tq'.format(edge['source']['nodeinfo']['hostname'],edge['target']['nodeinfo']['hostname'])
        except TypeError:
            pass
        else:
            values[key] = 1.0/edge['tq']

    return values

def yield_nodes(data):
    version = int(data.get('version', 0))
    if version == 2:
        for node in data['nodes']:
            yield node
        return
    elif version == 1:
        for mac, node in data['nodes'].items():
            yield node
        return
    raise RuntimeError("Invalid version: %i" % version)

def main():
    logging.basicConfig(level=logging.DEBUG)
    while True:
        pprinter = pprint.PrettyPrinter(indent=4)

        URL = 'https://www1.darmstadt.freifunk.net/data/nodes.json'

        gateways = []

        try:
            client_count = 0

            r = requests.get(URL, timeout=1, headers={'Host': 'map.darmstadt.freifunk.net'})
            print(r.headers)
            data = r.json()
            known_nodes = 0
            online_nodes = 0
            update = {} # parse_graph(nodes)
            gateway_count = 0
            for node in yield_nodes(data):
                known_nodes += 1
                try:
                    hostname = node['nodeinfo']['hostname']

                    flags = node['flags']
                    if flags['online']:
                        online_nodes += 1

                    if flags.get('gateway', False):
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
                    print('error while reading ', node, e)
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
