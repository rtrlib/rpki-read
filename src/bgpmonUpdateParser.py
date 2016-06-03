#!/usr/bin/python

import argparse
import calendar
import json
import logging
import os
import random
import re
import socket
import string
import sys
import time
import xml


import multiprocessing as mp
import xml.etree.ElementTree as ET

from datetime import datetime
from xml.dom import minidom

from settings import *

def parse_bgp_message(xml):
    """Returns a dict of a parsed BGP XML update message"""
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

def _init_bgpmon_sock(host, port):
    logging.debug ("CALL _init_bgpmon_sock")
    bm_sock = None
    ready = False
    timeout = 0
    while not ready:
        bm_sock =  socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            bm_sock.connect((host,port))
        except:
            bm_sock.close()
            backoff = pow(2,(timeout%6)+5)
            timeout += 1
            logging.critical ("Failed to connect to BGPmon XML RIB or UPDATE stream! Wait "+str(backoff)+"s and try again.")
            time.sleep(backoff)
        else:
            ready = True
    return bm_sock

def recv_bgpmon_rib(host, port, queue):
    """Read the RIB XML stream of bgpmon"""
    logging.info ("CALL recv_bgpmon_rib (%s:%d)", host, port)
    # open connection
    sock = _init_bgpmon_sock(host,port)
    data = ""
    stream = ""
    # receive data
    run = True
    parse = False
    logging.info("receiving XML RIB stream ...")
    while(run):
        data = sock.recv(1024)
        if not data:
            sock.close()
            time.sleep(60)
            sock = _init_bgpmon_sock(host,port)
            continue

        stream += data
        stream = string.replace(stream, "<xml>", "")
        while (re.search('</BGP_MONITOR_MESSAGE>', stream)):
            messages = stream.split('</BGP_MONITOR_MESSAGE>')
            msg = messages[0] + '</BGP_MONITOR_MESSAGE>'
            # stop RIB parsing after TABLE_STOP message
            if re.search('TABLE_STOP', msg):
                logging.info("found TABLE_STOP in XML RIB stream.")
                parse = False
            stream = '</BGP_MONITOR_MESSAGE>'.join(messages[1:])
            # parse RIB message if parsing is enabled
            if parse:
                result = parse_bgp_message(msg)
                if result:
                    queue.put(result)
            # start RIB parsing after TABLE_START message
            elif re.search('TABLE_START', msg):
                logging.info("found TABLE_START in XML RIB stream.")
                parse = True

    sock.close()
    return True

def recv_bgpmon_updates(host, port, queue):
    """Read the BGP update XML stream of bgpmon"""
    logging.info ("CALL recv_bgpmon_updates (%s:%d)", host, port)
    # open connection
    sock = _init_bgpmon_sock(host,port)
    data = ""
    stream = ""
    # receive data
    logging.info("receiving XML update stream ...")
    while(True):
        data = sock.recv(1024)
        if not data:
            sock.close()
            time.sleep(60)
            sock = _init_bgpmon_sock(host,port)
            continue
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
    """Output parsed BGP messages as JSON to STDOUT"""
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
    """The main loop"""
    parser = argparse.ArgumentParser(description='', epilog='')
    parser.add_argument('-l', '--loglevel',
                        help='Set loglevel [DEBUG,INFO,WARNING,ERROR,CRITICAL].',
                        type=str, default='ERROR')
    parser.add_argument('-a', '--addr',
                        help='Address or name of BGPmon host.',
                        type=str, default=DEFAULT_BGPMON_SERVER['host'])
    parser.add_argument('-p', '--port',
                        help='Port of BGPmon Update XML stream.',
                        type=int, default=DEFAULT_BGPMON_SERVER['uport'])
    parser.add_argument('-r', '--ribport',
                        help='Port of BGPmon RIB XML stream.',
                        type=int, default=DEFAULT_BGPMON_SERVER['rport'])
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
    rt = mp.Process(target=recv_bgpmon_rib,
                    args=(addr,args['ribport'], output_queue))
    try:
        ot.start()
        if args['ribport'] > 0:
            rt.start()
        recv_bgpmon_updates(addr,port,output_queue)
    except KeyboardInterrupt:
        logging.exception ("ABORT")
    finally:
        output_queue.put("STOP")

    if args['ribport'] > 0:
        rt.terminate()
    ot.join()
    logging.info("FINISH")
    # END

if __name__ == "__main__":
    main()
