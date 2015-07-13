#!/usr/bin/python

from __future__ import print_function

import argparse
import calendar
import json
import os
import re
import socket
import string
import sys
import xml

import multiprocessing as mp
from datetime import datetime
from subprocess import PIPE, Popen

from settings import *

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
                vrp['prefix'] = rprefix+"/"+rmax_len
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
    print_log("start validator thread")
    cache_cmd = [validator_path, cache_host, cache_port]
    validator_process = Popen(cache_cmd, stdin=PIPE, stdout=PIPE)
    print_log("CALL validator thread (%s:%s)" % (cache_host, cache_port))
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
        print_info(cache_host+":"+cache_port + " -> " + network+"/"+masklen +
                    "(AS"+asn+") -> " + validity['state'])
        return_data = dict()
        return_data['route'] = dict()
        return_data['route']['origin_asn'] = "AS"+asn
        return_data['route']['prefix'] = validation_entry[0]
        return_data['validity'] = validity
        out_queue.put({"type":"announcement", "validated_route":return_data})
    # end while
    validator_process.kill()
    return True

def output(queue, format_json):
    print_log("start output")
    while True:
        odata = queue.get()
        if (odata == 'STOP'):
            break
        try:
            if format_json:
                print(json.dumps(odata, sort_keys=True,
                                 indent=2, separators=(',', ': ')))
            else:
                print(json.dumps(odata))
        except Exception, e:
            print_error("output thread failed with: %s" % e.message)
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
                        help='Address or name of RPKI cache server.',
                        default=default_cache_server['host'])
    parser.add_argument('-p', '--port',
                        help='Port of RPKI cache server.',
                        default=default_cache_server['port'], type=int)
    parser.add_argument('-j', '--json',
                        help='Format JSON output nicely.',
                        action='store_true')
    args = vars(parser.parse_args())

    global verbose
    verbose   = args['verbose']
    global warning
    warning   = args['warning']
    global logging
    logging = args['logging']

    addr = args['addr'].strip()
    port = args['port']

    # BEGIN
    print_log(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + " starting ...")
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
            print_warn("Failed to parse JSON from input.")
        else:
            if data['type'] == 'announcement':
                path = data['path']
                if len(path) < 1:
                    continue
                origin = path[-1]
                prefixes = data['prefixes']
                for p in prefixes:
                    print_info (p+" : "+origin)
                    input_queue.put( (p, origin) )
            elif data['type'] == 'withdraw':
                prefixes = data['prefixes']
                for p in prefixes:
                    output_queue.put({"type":"withdraw", "prefix":p})

    input_queue.put("STOP")
    output_queue.put("STOP")


if __name__ == "__main__":
    main()
