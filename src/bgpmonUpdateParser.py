#!/usr/bin/python
import sys
import os
import json
import socket
import string
import re
import xml
import argparse
import calendar
import logging

import multiprocessing as mp
import xml.etree.ElementTree as ET

from xml.dom import minidom
from datetime import datetime

from settings import default_bgpmon_server

def parse_bgp_message(xml):
    logging.info("CALL parse_bgp_message")
    try:
        tree = ET.fromstring(xml)
    except:
        logging.exception ("Cannot parse XML: " + xml)
        return None
    logging.debug ("root: %s" % tree.tag)
    for child in tree:
        logging.debug (child.tag)

    # check if source exists, otherwise return
    src = tree.find('{urn:ietf:params:xml:ns:bgp_monitor}SOURCE')
    if src is None:
        logging.warning ("Invalid XML, no source!")
        return None
    src_peer = dict()
    src_peer['addr'] = src.find('{urn:ietf:params:xml:ns:bgp_monitor}ADDRESS').text
    src_peer['port'] = src.find('{urn:ietf:params:xml:ns:bgp_monitor}PORT').text
    src_peer['asn'] = src.find('{urn:ietf:params:xml:ns:bgp_monitor}ASN2').text

    # get timestamp
    dt = tree.find('{urn:ietf:params:xml:ns:bgp_monitor}OBSERVED_TIME')
    if dt is None:
        logging.warning ("Invalid XML, no source!")
        return None
    ts = dt.find('{urn:ietf:params:xml:ns:bgp_monitor}TIMESTAMP').text


    # check wether it is a keep alive message
    keep_alive = tree.find('{urn:ietf:params:xml:ns:xfb}KEEP_ALIVE')
    if keep_alive is not None:
        logging.debug ("BGP KEEP ALIVE %s (AS %s)" % (src_peer['addr'], src_peer['asn']))
        return None

    # proceed with bgp update parsing
    update = tree.find('{urn:ietf:params:xml:ns:xfb}UPDATE')
    if update is None:
        logging.warning ("Invalid XML, no update!")
        return None

    # init return struct
    bgp_message = dict()
    bgp_message['type'] = 'update'
    bgp_message['source'] = src_peer
    bgp_message['next_hop'] = None
    bgp_message['timestamp'] = str(ts)
    bgp_message['announce'] = list()
    bgp_message['withdraw'] = list()
    bgp_message['aspath'] = list()

    # add withdrawn prefixes
    withdraws = update.findall('.//{urn:ietf:params:xml:ns:xfb}WITHDRAW')
    for withdraw in withdraws:
        logging.debug ("BGP WITHDRAW %s by AS %s" % (withdraw.text,src_peer['asn']))
        bgp_message['withdraw'].append(str(withdraw.text))

    # add AS path
    asp = update.find('{urn:ietf:params:xml:ns:xfb}AS_PATH')
    if asp is not None:
        for asn in asp.findall('.//{urn:ietf:params:xml:ns:xfb}ASN2'):
                bgp_message['aspath'].append(str(asn.text))

    # add next hop
    next_hop = update.find('{urn:ietf:params:xml:ns:xfb}NEXT_HOP')
    if next_hop is not None:
        bgp_message['next_hop'] = next_hop.text

    # add announced prefixes
    prefixes = update.findall('.//{urn:ietf:params:xml:ns:xfb}NLRI')
    for prefix in prefixes:
        logging.debug ("BGP ANNOUNCE %s by AS %s" % (prefix.text,src_peer['asn']))
        bgp_message['announce'].append(str(prefix.text))

    return bgp_message

def recv_bgpmon_messages(host,port, queue):
    logging.info ("CALL recv_bgpmon_updates (%s:%d)", host, port)
    # open connection
    sock =  socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((host,port))
    except:
        logging.critical ("Failed to connect to BGPmon!")
        sys.exit(1)

    data = ""
    stream = ""
    # receive data
    while(True):
        data = sock.recv(1024)
        stream += data
        stream = string.replace(stream, "<xml>", "")
        while (re.search('</BGP_MONITOR_MESSAGE>', stream)):
            messages = stream.split('</BGP_MONITOR_MESSAGE>')
            msg = messages[0] + '</BGP_MONITOR_MESSAGE>'
            stream = '</BGP_MONITOR_MESSAGE>'.join(messages[1:])
            result = parse_bgp_message(msg)
            if result:
                queue.put(result)
    return True

def output(queue):
    logging.info ("CALL output")
    while True:
        odata = queue.get()
        if (odata == 'STOP'):
            break
        json_str = json.dumps(odata)
        print json_str
        sys.stdout.flush()
    return True

def main():
    parser = argparse.ArgumentParser(description='', epilog='')
    parser.add_argument('-l', '--loglevel',
                        help='Set loglevel [DEBUG,INFO,WARNING,ERROR,CRITICAL].',
                        type=str, default='WARNING')
    parser.add_argument('-a', '--addr',
                        help='Address or name of BGPmon host.',
                        default=default_bgpmon_server['host'])
    parser.add_argument('-p', '--port',
                        help='Port of BGPmon Update XML stream.',
                        default=default_bgpmon_server['port'], type=int)
    args = vars(parser.parse_args())

    numeric_level = getattr(logging, args['loglevel'].upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % loglevel)
    logging.basicConfig(level=numeric_level,
                        format='%(asctime)s : %(levelname)s : %(message)s')

    port = args['port']
    addr = args['addr'].strip()

    logging.info("START")

    output_queue = mp.Queue()
    ot = mp.Process(target=output,
                    args=(output_queue,))
    try:
        ot.start()
        recv_bgpmon_messages(addr,port,output_queue)
    except KeyboardInterrupt:
        logging.exception ("ABORT")
    finally:
        output_queue.put("STOP")

    ot.join()
    logging.info("FINISH")
    # END

if __name__ == "__main__":
    main()
