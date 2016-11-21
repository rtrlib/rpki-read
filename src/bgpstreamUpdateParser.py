#!/usr/bin/python
from __future__ import print_function
import argparse
import json
import logging
import os
import random
import re
import socket
import string
import sys
import time

from datetime import datetime, date, timedelta
from _pybgpstream import BGPStream, BGPRecord, BGPElem

from settings import *
from BGPmessage import *

# helper functions
def valid_date(s):
    """
    Verifies date parameters for correct input format
    """
    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M")
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)

def output(odata):
    """
    Output parsed BGP messages as JSON to STDOUT
    """
    if (odata == 'STOP'):
        print(odata)
    else:
        print(json.dumps(odata.__dict__))
    # end if
    sys.stdout.flush()

def wait_to_sync():
    while(os.path.exists(WAIT_TO_SYNC_FILE)):
        time.sleep(1)

def recv_bgpstream_rib(begin, until, collector):
    """
    Receive and parse BGP RIB records from a given bgpstream collector.
    """
    logging.info ("CALL recv_bgpstream_rib")
    # Create bgpstream
    stream = BGPStream()
    rec = BGPRecord()
    # set filtering
    stream.add_filter('collector', collector)
    stream.add_filter('record-type','updates')
    stream.add_interval_filter(begin,until)

    # Start the stream
    stream.start()
    while (stream.get_next_record(rec)):
        wait_to_sync()
        if rec.status == 'valid':
            elem = rec.get_next_elem()
        else:
            logging.warn("stream record invalid, skipping.")
            continue
        bgp_message = None
        while (elem):
            if (elem.type.upper() == 'A') or (elem.type.upper() == 'R'):
                bgp_message = BGPmessage(elem.time, 'update')
                bgp_message.set_nexthop(elem.fields['next-hop'])
                src_peer = dict()
                src_addr = elem.peer_address
                src_asn = elem.peer_asn
                src_peer['addr'] = src_addr
                src_peer['port'] = 0
                src_peer['asn'] = src_asn
                bgp_message.set_source(src_peer)
                aspath = elem.fields['as-path'].split()
                for a in aspath:
                    if not '{' in a: # ignore AS-SETs
                        bgp_message.add_as_to_path(a)
                bgp_message.add_announce(elem.fields['prefix'])
                output(bgp_message)
            elem = rec.get_next_elem()
        # end while (elem)
    # end while (stream...)

def recv_bgpstream_updates(begin, until, collector):
    """
    Receive and parse BGP update records from a given bgpstream collector
    """
    logging.info ("CALL recv_bgpstream_updates")
    # Create bgpstream
    stream = BGPStream()
    rec = BGPRecord()
    # set filtering
    stream.add_filter('collector', collector)
    stream.add_filter('record-type','updates')
    stream.add_interval_filter(begin, until)
    # Start the stream
    stream.start()
    while (stream.get_next_record(rec)):
        wait_to_sync()
        if rec.status == 'valid':
            elem = rec.get_next_elem()
        else:
            logging.warn("stream record invalid, skipping ...")
            continue
        logging.info("Record TS: "+str(rec.time))
        while (elem):
            logging.info(" -- Record Element Type: " + elem.type + ", TS: " + str(elem.time))
            bgp_message = BGPmessage(elem.time, 'update')
            src_peer = dict()
            src_addr = elem.peer_address
            src_asn = elem.peer_asn
            src_peer['addr'] = src_addr
            src_peer['port'] = 0
            src_peer['asn'] = src_asn
            bgp_message.set_source(src_peer)
            if elem.type.upper() == 'A':
                bgp_message.add_announce(elem.fields['prefix'])
                bgp_message.set_nexthop(elem.fields['next-hop'])
                aspath = elem.fields['as-path'].split()
                for a in aspath:
                    if not '{' in a: # ignore AS-SETs
                        bgp_message.add_as_to_path(a)
                output(bgp_message)
            elif elem.type.upper() == 'W':
                bgp_message.add_withdraw(elem.fields['prefix'])
                output(bgp_message)
            elem = rec.get_next_elem()
        # end while (elem)
    # end while (stream...)

def main():
    """
    The main loop, parsing arguments and start input and output threads loop
    """
    parser = argparse.ArgumentParser(description='', epilog='')
    parser.add_argument('-b', '--begin',
                        help='Begin date (inclusive), format: yyyy-mm-dd HH:MM',
                        type=valid_date, default=datetime.now())
    parser.add_argument('-u', '--until',
                        help='Until date (exclusive), format: yyyy-mm-dd HH:MM',
                        type=valid_date, required=False)
    parser.add_argument('-c', '--collector',
                        help='Route collector from RIPE RIS or Route-Views project.',
                        type=str, default=DEFAULT_BGPSTREAM_COLLECTOR)
    parser.add_argument('-l', '--loglevel',
                        help='Set loglevel [DEBUG,INFO,WARNING,ERROR,CRITICAL].',
                        type=str, default=DEFAULT_LOG_LEVEL)
    args = vars(parser.parse_args())

    numeric_level = getattr(logging, args['loglevel'].upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % loglevel)
    logging.basicConfig(level=numeric_level,
                        format='%(asctime)s : %(levelname)s : %(message)s')

    # parse and init timestamps
    dt_begin = args['begin']
    ts_begin = int(time.mktime(dt_begin.timetuple()))
    ts_until = 0
    if args['until']:
        dt_until = args['until']
        ts_until = int(time.mktime(dt_until.timetuple()))
    # start
    logging.info("START ("+str(ts_begin)+" - "+str(ts_until)+")")
    if os.path.exists(WAIT_TO_SYNC_FILE):
        try:
            logging.info("remove wait_to_sync file")
            os.remove(WAIT_TO_SYNC_FILE)
        except Exception as errmsg:
            logging.exception("remove wait_to_sync file, failed with: " + str(errmsg))
        # end try
    # end if
    try:
        # receive last full RIB first
        recv_bgpstream_rib( (ts_begin - RIB_TS_INTERVAL), ts_begin, args['collector'])
        # receive updates
        recv_bgpstream_updates(ts_begin, ts_until, args['collector'])
    except KeyboardInterrupt:
        logging.exception("ABORT")
    finally:
        output("STOP")
    logging.info("FINISH")
    # END

if __name__ == "__main__":
    main()
