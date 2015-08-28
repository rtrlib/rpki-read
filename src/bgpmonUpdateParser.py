#!/usr/bin/python

from __future__ import print_function
import sys
import os
import json
import socket
import string
import re
import xml
import argparse
import calendar

import multiprocessing as mp
import xml.etree.ElementTree as ET

from xml.dom import minidom
from datetime import datetime

from settings import default_bgpmon_server

verbose = False
warning = False
logging = False

def print_log(*objs):
    if logging or verbose:
        print("[LOGS] ", *objs, file=sys.stderr)

def print_info(*objs):
    if verbose:
        print("[INFO] ", *objs, file=sys.stderr)

def print_warn(*objs):
    if warning or verbose:
        print("[WARN] ", *objs, file=sys.stderr)

def print_error(*objs):
    print("[ERROR] ", *objs, file=sys.stderr)

def parse_bgp_message(xml):
    try:
        tree = ET.fromstring(xml)
    except:
        print_error("Cannot parse XML: %s!" % xml)
        return None
    print_info("root: %s" % tree.tag)
    for child in tree:
        print_info(child.tag)

    # check if source exists, otherwise return
    src = tree.find('{urn:ietf:params:xml:ns:bgp_monitor}SOURCE')
    if src is None:
        print_warn("Invalid XML, no source!")
        return None
    src_peer = dict()
    src_peer['addr'] = src.find('{urn:ietf:params:xml:ns:bgp_monitor}ADDRESS').text
    src_peer['port'] = src.find('{urn:ietf:params:xml:ns:bgp_monitor}PORT').text
    src_peer['asn'] = src.find('{urn:ietf:params:xml:ns:bgp_monitor}ASN2').text

    # get timestamp
    dt = tree.find('{urn:ietf:params:xml:ns:bgp_monitor}OBSERVED_TIME')
    if dt is None:
        print_error("Invalid XML, no source!")
        return None
    ts = dt.find('{urn:ietf:params:xml:ns:bgp_monitor}TIMESTAMP').text


    # check wether it is a keep alive message
    keep_alive = tree.find('{urn:ietf:params:xml:ns:xfb}KEEP_ALIVE')
    if keep_alive is not None:
        print_log("BGP KEEP ALIVE %s (AS %s)" % (src_peer['addr'], src_peer['asn']))
        return None

    # proceed with bgp update parsing
    update = tree.find('{urn:ietf:params:xml:ns:xfb}UPDATE')
    if update is None:
        print_error("Invalid XML, no update!")
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
        print_info("BGP WITHDRAW %s by AS %s" % (withdraw.text,src_peer['asn']))
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
        print_info("BGP ANNOUNCE %s by AS %s" % (prefix.text,src_peer['asn']))
        bgp_message['announce'].append(str(prefix.text))

    return bgp_message

def recv_bgpmon_messages(host,port, queue):
    print_log("CALL recv_bgpmon_updates (%s:%d)" % (host,port))
    # open connection
    sock =  socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((host,port))
    except:
        print_error("Failed to connect to BGPmon!")
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
    print_log("CALL output")
    while True:
        odata = queue.get()
        if (odata == 'STOP'):
            break
        json_str = json.dumps(odata)
        print(json_str, file=sys.stdout)
        sys.stdout.flush()
    return True

def main():
    parser = argparse.ArgumentParser(description='', epilog='')
    parser.add_argument('-l', '--logging',
                        help='Ouptut log info.', action='store_true')
    parser.add_argument('-w', '--warning',
                        help='Output warnings.', action='store_true')
    parser.add_argument('-v', '--verbose',
                        help='Verbose output.', action='store_true')
    parser.add_argument('-a', '--addr',
                        help='Address or name of BGPmon host.',
                        default=default_bgpmon_server['host'])
    parser.add_argument('-p', '--port',
                        help='Port of BGPmon Update XML stream.',
                        default=default_bgpmon_server['port'], type=int)
    args = vars(parser.parse_args())

    global verbose
    verbose   = args['verbose']
    global warning
    warning   = args['warning']
    global logging
    logging = args['logging']

    # BEGIN
    print_log(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + " starting ...")

    port = args['port']
    addr = args['addr'].strip()

    output_queue = mp.Queue()
    ot = mp.Process(target=output,
                    args=(output_queue,))
    try:
        ot.start()
        recv_bgpmon_messages(addr,port,output_queue)
    except KeyboardInterrupt:
        print_warn("ABORT")
    finally:
        output_queue.put("STOP")

    ot.join()

    print_log(datetime.now().strftime('%Y-%m-%d %H:%M:%S') +  " done ...")
    # END

if __name__ == "__main__":
    main()
