#!/usr/bin/python
from __future__ import print_function

import argparse
import json
import logging
import re
import socket
import sys
import time

import multiprocessing as mp
import xml.etree.ElementTree as ET

from settings import DEFAULT_LOG_LEVEL, DEFAULT_BGPMON_SERVER
from BGPmessage import BGPmessage

def parse_bgp_message(xml):
    """
    Returns a dict of a parsed BGP XML update message
    """
    logging.info("CALL parse_bgp_message")
    try:
        tree = ET.fromstring(xml)
    except ET.ParseError:
        logging.exception("Cannot parse XML: " + xml)
        return None
    logging.debug("root: " + str(tree.tag))
    for child in tree:
        logging.debug (child.tag)

    # check if source exists, otherwise return
    src = tree.find('{urn:ietf:params:xml:ns:bgp_monitor}SOURCE')
    if src is None:
        logging.warning("Invalid XML, no source!")
        return None
    src_peer = dict()
    src_peer['addr'] = src.find('{urn:ietf:params:xml:ns:bgp_monitor}ADDRESS').text
    src_peer['port'] = src.find('{urn:ietf:params:xml:ns:bgp_monitor}PORT').text
    src_peer['asn'] = src.find('{urn:ietf:params:xml:ns:bgp_monitor}ASN2').text

    # get timestamp
    dt = tree.find('{urn:ietf:params:xml:ns:bgp_monitor}OBSERVED_TIME')
    if dt is None:
        logging.warning("Invalid XML, no source!")
        return None
    ts = dt.find('{urn:ietf:params:xml:ns:bgp_monitor}TIMESTAMP').text

    # check wether it is a keep alive message
    keep_alive = tree.find('{urn:ietf:params:xml:ns:xfb}KEEP_ALIVE')
    if keep_alive is not None:
        logging.debug("BGP KEEP ALIVE " + src_peer['addr'] + " (AS " + src_peer['asn'] + ")")
        return None

    # proceed with bgp update parsing
    update = tree.find('{urn:ietf:params:xml:ns:xfb}UPDATE')
    if update is None:
        logging.warning("Invalid XML, no update!")
        return None

    # init return struct
    bgp_message = BGPmessage(ts,'update')
    bgp_message.set_source(src_peer)

    # add withdrawn prefixes
    withdraws = update.findall('.//{urn:ietf:params:xml:ns:xfb}WITHDRAW')
    for withdraw in withdraws:
        logging.debug("BGP WITHDRAW " + withdraw.text + " (AS" + src_peer['asn'] + ")")
        bgp_message.add_withdraw(withdraw.text)

    # add AS path
    asp = update.find('{urn:ietf:params:xml:ns:xfb}AS_PATH')
    if asp is not None:
        for asn in asp.findall('.//{urn:ietf:params:xml:ns:xfb}ASN2'):
            bgp_message.add_as_to_path(asn.text)

    # add next hop
    next_hop = update.find('{urn:ietf:params:xml:ns:xfb}NEXT_HOP')
    if next_hop is not None:
        bgp_message.set_nexthop(next_hop.text)

    # add announced prefixes
    prefixes = update.findall('.//{urn:ietf:params:xml:ns:xfb}NLRI')
    for prefix in prefixes:
        logging.debug("BGP ANNOUNCE %s by AS %s" % (prefix.text, src_peer['asn']))
        bgp_message.add_announce(prefix.text)

    return bgp_message

def _init_bgpmon_sock(host, port):
    """
    Init bgpmon socket connections
    """
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
    """
    Receive and parse the BGP RIB XML stream of bgpmon
    """
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
        stream = str.replace(stream, "<xml>", "")
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
    """
    Receive and parse the BGP update XML stream of bgpmon
    """
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
        stream = str.replace(stream, "<xml>", "")
        while (re.search('</BGP_MONITOR_MESSAGE>', stream)):
            messages = stream.split('</BGP_MONITOR_MESSAGE>')
            msg = messages[0] + '</BGP_MONITOR_MESSAGE>'
            stream = '</BGP_MONITOR_MESSAGE>'.join(messages[1:])
            result = parse_bgp_message(msg)
            if result:
                queue.put(result)
    return True

def output(queue):
    """
    Output parsed BGP messages as JSON to STDOUT
    """
    logging.info ("CALL output")
    run = True
    while run is True:
        odata = queue.get()
        if (odata == 'STOP'):
            print(odata)
            run = False
        else:
            print(json.dumps(odata.__dict__))
        # end if
        sys.stdout.flush()
    # end while
    return True

def main():
    """
    The main loop, parsing arguments and start input and output threads
    """
    parser = argparse.ArgumentParser(description='', epilog='')
    parser.add_argument('-l', '--loglevel',
                        help='Set loglevel [DEBUG,INFO,WARNING,ERROR,CRITICAL].',
                        type=str, default=DEFAULT_LOG_LEVEL)
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
        raise ValueError('Invalid log level: %s' % args['loglevel'])
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
