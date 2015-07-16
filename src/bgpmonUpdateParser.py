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

def parse_update(xml, filter):
    try:
        tree = ET.fromstring(xml)
    except:
        print_error("Cannot parse XML: %s!" % xml)
        return None
    print_info("root: %s" % tree.tag)
    for child in tree:
        print_info(child.tag)
    src = tree.find('{urn:ietf:params:xml:ns:bgp_monitor}SOURCE')
    # check if source exists, otherwise return
    if src is None:
        print_warn("Invalid XML, no source!")
        return None
    # find source
    src_addr = src.find('{urn:ietf:params:xml:ns:bgp_monitor}ADDRESS').text
    src_asn = src.find('{urn:ietf:params:xml:ns:bgp_monitor}ASN2').text
    # init return struct
    bgp_message = dict()
    bgp_message['type'] = 'announcement'
    bgp_message['asn'] = str(src_asn)
    bgp_message['prefixes'] = list()
    bgp_message['path'] = list()

    # check wether it is a keep alive message
    keep_alive = tree.find('{urn:ietf:params:xml:ns:xfb}KEEP_ALIVE')
    if keep_alive is not None:
        print_log("BGP KEEP ALIVE %s (AS %s)" % (src_addr, src_asn))
        return None
    # proceed with bgp update parsing
    update = tree.find('{urn:ietf:params:xml:ns:xfb}UPDATE')
    if update is None:
        print_warn("Invalid XML, no update!")
        return None

    # check if its a bgp withdraw message
    withdraws = update.findall('.//{urn:ietf:params:xml:ns:xfb}WITHDRAW')
    for withdraw in withdraws:
        bgp_message['type'] = 'withdraw'
        prefix = withdraw.text
        print_log("BGP WITHDRAW %s by AS %s" % (prefix, src_asn))
        bgp_message['prefixes'].append(str(prefix))
    if bgp_message['type'] == 'withdraw':
        return bgp_message

    asp = update.find('{urn:ietf:params:xml:ns:xfb}AS_PATH')
    if asp is not None:
        for asn in asp.findall('.//{urn:ietf:params:xml:ns:xfb}ASN2'):
                bgp_message['path'].append(str(asn.text))

    if filter and (len(bgp_message['path']) > 0):
        origin = bgp_message['path'][-1]
        if origin not in filter:
            print_warn("Filter mismatch, origin AS: " + origin)
            return None

    #next_hop = update.find('{urn:ietf:params:xml:ns:xfb}NEXT_HOP').text
    prefixes = update.findall('.//{urn:ietf:params:xml:ns:xfb}NLRI')
    for prefix in prefixes:
        bgp_message['prefixes'].append(str(prefix.text))

    return bgp_message

def read_filter(fin):
    print_log ("CALL read_filter (%s)." % fin)
    if not os.path.isfile(fin):
        print_warn("not a file")
        return None
    lines = [line.strip() for line in open(fin)]
    filter = set()
    for l in lines:
        asn = l.split(',')
        try:
            t = int(asn)
        except:
            pass
        else:
            filter.add(asn)
    if len(filter) > 0:
        return filter
    # found nothing, so return None
    return None

def recv_bgpmon_updates(host,port,filter, queue):
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
            result = parse_update(msg, filter)
            if result:
                queue.put(result)
    return True

def output(queue):
    print_log("CALL output (%s)" % format)
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
    fgroup = parser.add_mutually_exclusive_group(required=False)
    fgroup.add_argument('-f', '--filter',
                        help="ASN filter, as comma separated list.",
                        type=str, default=None)
    fgroup.add_argument('-r', '--readfilter',
                        help="ASN filter, read from csv file.",
                        type=str, default=None)
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
    filter = None
    if args['filter']:
        filter = args['filter'].split(',')
    if args['readfilter']:
        filter = read_filter(args['readfilter'])

    output_queue = mp.Queue()
    ot = mp.Process(target=output,
                    args=(output_queue,))
    try:
        ot.start()
        recv_bgpmon_updates(addr,port,filter,output_queue)
    except KeyboardInterrupt:
        print_warn("ABORT")
    finally:
        output_queue.put("STOP")

    ot.join()

    print_log(datetime.now().strftime('%Y-%m-%d %H:%M:%S') +  " done ...")
    # END

if __name__ == "__main__":
    main()
