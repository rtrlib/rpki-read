#!/usr/bin/python
import argparse
import calendar
import json
import logging
import os
import re
import socket
import string
import sys

import multiprocessing as mp
from datetime import datetime
from subprocess import PIPE, Popen

# internal imports
from settings import *

def _get_validity(validation_result_string):
    validity = dict()
    validity['code'] = 100
    validity['state'] = 'Error'
    validity['description'] = 'Unknown validation error.'

    # check validation result
    validation_result_array = validation_result_string.split("|")
    if validation_result_string == "error":
        validity['code'] = 101
        validity['description'] = 'RPKI cache-server connection failure!'
    elif validation_result_string == "timeout":
        validity['code'] = 102
        validity['description'] = 'RPKI cache-server connection timeout!'
    elif validation_result_string == "input error":
        validity['code'] = 103
        validity['description'] = 'RPKI cache-server input error!'
    elif len(validation_result_array) != 3:
        validity['code'] = 104
        validity['description'] = 'RPKI cache-server output error!'
    else: # looks like a valid validation result string
        query = validation_result_array[0]
        reasons = validation_result_array[1]
        validity['code'] = int(validation_result_array[2])

        validity['VRPs'] = dict()
        validity['VRPs']['matched'] = list()
        validity['VRPs']['unmatched_as'] = list()
        validity['VRPs']['unmatched_length'] = list()
        if validity['code'] != 1:
            reasons_array = reasons.split(',')
            vprefix, vlength, vasn = query.split()
            for r in reasons_array:
                rasn, rprefix, rmin_len, rmax_len = r.split()
                vrp = dict()
                vrp['asn'] = "AS"+rasn
                vrp['prefix'] = rprefix+"/"+rmin_len
                vrp['max_length'] = rmax_len
                match = True
                if vasn != rasn:
                    validity['VRPs']['unmatched_as'].append(vrp)
                    match = False
                if vlength > rmax_len:
                    validity['VRPs']['unmatched_length'].append(vrp)
                    match = False
                if match:
                    validity['VRPs']['matched'].append(vrp)
            # END (for r in reasons_array)
            if validity['code'] == 2:
                if len(reasons_array) == len(validity['VRPs']['unmatched_as']):
                    validity['code'] = 3
                    validity['reason'] = 'as'
                elif len(reasons_array) == len(validity['VRPs']['unmatched_length']):
                    validity['code'] = 4
                    validity['reason'] = 'length'
            # END (if validity['code'] == 2)
        # END (if validity['code'] != 1)
        validity['state'] = validity_state[validity['code']]
        validity['description'] = validity_descr[validity['code']]
    # END (if elif else)
    return validity

def validator(in_queue, out_queue, cache_host, cache_port):
    logging.info ("start validator thread")
    cache_cmd = [validator_path, cache_host, cache_port]
    validator_process = Popen(cache_cmd, stdin=PIPE, stdout=PIPE)
    logging.info ("run validator thread (%s:%s)" % (cache_host, cache_port))
    run = True
    while run:
        validation_entry = in_queue.get(True)
        if validation_entry == "STOP":
            run = False
            break
        network, masklen    = validation_entry[0].split('/')
        asn                 = validation_entry[1]
        bgp_entry_str = str(network) + " " + str(masklen) + " " + str(asn)

        validator_process.stdin.write(bgp_entry_str + '\n')
        validation_result = validator_process.stdout.readline().strip()
        validity =  _get_validity(validation_result)
        logging.debug (cache_host+":"+cache_port + " -> " + network+"/"+masklen +
                    "(AS"+asn+") -> " + validity['state'])
        return_data = dict()
        return_data['route'] = dict()
        return_data['route']['origin_asn'] = "AS"+asn
        return_data['route']['prefix'] = validation_entry[0]
        return_data['validity'] = validity
        out_queue.put({ "type":"announcement",
                        "source":validation_entry[2], # source
                        "timestamp":validation_entry[3], # timestamp
                        "next_hop":validation_entry[4], # next_hop
                        "validated_route":return_data
                        })
    # end while
    validator_process.kill()
    return True

def output(queue, format_json):
    logging.info ("start output")
    while True:
        odata = queue.get()
        if (odata == 'STOP'):
            break
        try:
            if format_json:
                print json.dumps(odata, sort_keys=True,
                                 indent=2, separators=(',', ': '))
            else:
                print json.dumps(odata)
        except Exception, e:
            logging.exception ("output thread failed with: %s", e.message)
    return True

def main():
    parser = argparse.ArgumentParser(description='', epilog='')
    parser.add_argument('-l', '--loglevel',
                        help='Set loglevel [DEBUG,INFO,WARNING,ERROR,CRITICAL].',
                        type=str, default='WARNING')
    parser.add_argument('-a', '--addr',
                        help='Address or name of RPKI cache server.',
                        default=default_cache_server['host'])
    parser.add_argument('-p', '--port',
                        help='Port of RPKI cache server.',
                        default=default_cache_server['port'], type=int)
    parser.add_argument('-j', '--json',
                        help='Format JSON output nicely.',
                        action='store_true')
    args = vars(parser.parse_args())

    numeric_level = getattr(logging, args['loglevel'].upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % loglevel)
    logging.basicConfig(level=numeric_level,
                        format='%(asctime)s : %(levelname)s : %(message)s')

    addr = args['addr'].strip()
    port = args['port']

    # BEGIN
    logging.info ("START")
    # init queues
    input_queue = mp.Queue()
    output_queue = mp.Queue()
    # start validator thread
    vt = mp.Process(target=validator,
                    args=(input_queue,output_queue,addr,str(port)))
    vt.start()
    # start output thread
    ot = mp.Process(target=output,
                    args=(output_queue,args['json']))
    ot.start()
    # main loop
    while True:
        line = sys.stdin.readline().strip()
        try:
            data = json.loads(line)
        except:
            logging.exception ("Failed to parse JSON from input.")
        else:
            if data['type'] == 'update':
                withdraws = data['withdraw']
                for w in withdraws:
                    output_queue.put({  "type":"withdraw",
                                        "prefix":w,
                                        "source":data['source'],
                                        "timestamp":data['timestamp']
                                      })
                path = data['aspath']
                if len(path) < 1:
                    continue
                origin = path[-1]
                prefixes = data['announce']
                for p in prefixes:
                    logging.debug (p+" : "+origin)
                    input_queue.put( (  p, origin,
                                        data['source'],
                                        data['timestamp'],
                                        data['next_hop']) )

    input_queue.put("STOP")
    output_queue.put("STOP")
    logging.info("FINISH")


if __name__ == "__main__":
    main()
