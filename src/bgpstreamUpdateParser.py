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

import multiprocessing as mp

from datetime import datetime, date, timedelta
from _pybgpstream import BGPStream, BGPRecord, BGPElem

from settings import *
from BGPmessage import *

# helper functions
def valid_date(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M")
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)

def output(queue):
    """Output parsed BGP messages as JSON to STDOUT"""
    logging.info ("CALL output")
    while True:
        odata = queue.get()
        if (odata == 'STOP'):
            break
        json_str = json.dumps(odata.__dict__)
        print json_str
        sys.stdout.flush()
    return True
def recv_bgpstream_updates(begin, until, collector,output_queue):
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
        if rec.status == 'valid':
            elem = rec.get_next_elem()
        else:
            logging.warn("stream record invalid, skipping ...")
            continue
        logging.info("Record TS: "+str(rec.time))
        while (elem):
            logging.info(" -- Record Element Type: " + elem.type + ", TS: " + str(elem.time))
            bgp_message = BGPmessage(elem.time,'update')
            if elem.type == 'announcement':
                bgp_message.add_announce(elem.fields['prefix'])
                bgp_message.set_nexthop(elem.fields['next-hop'])
                aspath = elem.fields['as-path'].split()
                for a in aspath: # remove AS-SETs
                    bgp_message.add_as_to_path(a)
                output_queue.put(bgp_message)
            elif elem.type == 'withdrawal':
                bgp_message.add_withdraw(elem.fields['prefix'])
                output_queue.put(bgp_message)
            elem = rec.get_next_elem()
        # end while (elem)
    # end while (stream...)

def main():
    """The main loop"""
    parser = argparse.ArgumentParser(description='', epilog='')
    parser.add_argument('-b', '--begin',
                        help='Begin date (inclusive), format: yyyy-mm-dd HH:MM',
                        type=valid_date, required=False)
    parser.add_argument('-u', '--until',
                        help='Until date (exclusive), format: yyyy-mm-dd HH:MM',
                        type=valid_date, required=False)
    parser.add_argument('-c', '--collector',
                        help='Route collector from RIPE RIS or Route-Views project.',
                        type=str, required=True)
    parser.add_argument('-l', '--loglevel',
                        help='Set loglevel [DEBUG,INFO,WARNING,ERROR,CRITICAL].',
                        type=str, default='ERROR')
    args = vars(parser.parse_args())

    numeric_level = getattr(logging, args['loglevel'].upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % loglevel)
    logging.basicConfig(level=numeric_level,
                        format='%(asctime)s : %(levelname)s : %(message)s')

    ts_begin = int((datetime.now() - datetime(1970, 1, 1)).total_seconds())
    if args['begin']:
        ts_begin = int((args['begin'] - datetime(1970, 1, 1)).total_seconds())
    ts_until = 0
    if args['until']:
        ts_until = int((args['until'] - datetime(1970, 1, 1)).total_seconds())
    logging.info("START")

    output_queue = mp.Queue()
    ot = mp.Process(target=output,
                    args=(output_queue,))
    try:
        ot.start()
        recv_bgpstream_updates(ts_begin, ts_until, collector, output_queue)
    except KeyboardInterrupt:
        logging.exception ("ABORT")
    finally:
        output_queue.put("STOP")

    ot.join()
    logging.info("FINISH")
    # END

if __name__ == "__main__":
    main()
