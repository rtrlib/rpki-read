#!/usr/bin/python

from __future__ import print_function

import argparse
import calendar
import json
import os
import psycopg2
import re
import socket
import string
import sys

import multiprocessing as mp
from datetime import datetime
from subprocess import PIPE, Popen

from settings import *

verbose = False
warning = False
logging = False
keepwithdrawn = False

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

def outputPostgres(dbconnstr, queue):
    print_info(dbconnstr)
    try:
        con = psycopg2.connect(dbconnstr)
    except Exception, e:
        print_error("failed with: %s" % (e.message))
        print_error("connecting to database")
        sys.exit(1)
    cur = con.cursor()
    update_validity =   "UPDATE t_validity SET state='%s', ts='%s', " \
                        "roa_prefix='%s', roa_maxlen=%s, roa_asn=%s, " \
                        "next_hop='%s', src_asn=%s, src_addr='%s' " \
                        "WHERE prefix='%s'"
    insert_validity =   "INSERT INTO t_validity (prefix, origin, state, ts, roa_prefix, roa_maxlen, roa_asn, next_hop, src_asn, src_addr) " \
                        "SELECT '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s' " \
                        "WHERE NOT EXISTS (SELECT 1 FROM t_validity WHERE prefix='%s')"
    delete_validty =    "DELETE FROM t_validity WHERE prefix='%s'"
    while True:
        data = queue.get()
        if (data == 'DONE'):
            break
        try:
            if data['type'] == 'announcement':
                vr = data['validated_route']
                rt = vr['route']
                vl = vr['validity']
                vp = vl['VRPs']
                src = data['source']
                roa = {'prefix':'0.0.0.0', 'maxlen':0, 'asn':0}
                if vl['code'] == 0:
                    roa = vp['matched']
                elif vl['code'] == 3:
                    roa = vp['unmatched_as']
                elif vl['code'] == 4:
                    roa = vp['unmatched_length']

                ts_str = datetime.fromtimestamp(
                        int(data['timestamp'])).strftime('%Y-%m-%d %H:%M:%S')
                print_info("converted unix timestamp: " + ts_str)
                update_str = update_validity % (vl['state'], ts_str,
                    roa['prefix'], roa['maxlen'], roa['asn'],
                    data['next_hop'], src['asn'], src['addr'],
                    rt['prefix'])
                print_info("UPDATE: " + update_str)
                insert_str = insert_validity % (rt['prefix'],
                    rt['origin_asn'][2:], vl['state'], ts_str,
                    roa['prefix'], roa['maxlen'], roa['asn'],
                    data['next_hop'], src['asn'], src['addr'],
                    rt['prefix'])
                print_info("INSERT: " + insert_str)
                try:
                    cur.execute(update_str)
                    cur.execute(insert_str)
                    con.commit()
                except Exception, e:
                    print_error("updating or inserting entry, announcement")
                    print_error("... failed with: %s" % (e.message))
                    con.rollback()
            elif (data['type'] == 'withdraw'):
                if keepwithdrawn:
                    ts_str = datetime.fromtimestamp(
                        int(data['timestamp'])).strftime('%Y-%m-%d %H:%M:%S')
                    print_info("converted unix timestamp: " + ts_str)
                    src = data['source']
                    update_str = update_validity % ('withdrawn', ts_str, None,
                        None, None, None, src['asn'], src['addr'],
                    data['prefix'])
                    print_info("UPDATE: " + update_str)
                    try:
                        cur.execute(update_str)
                        con.commit()
                    except Exception, e:
                        print_error("updating entry, withdraw")
                        print_error("... failed with: %s" % (e.message))
                        con.rollback()
                else:
                    try:
                        cur.execute(delete_validty, [data['prefix']])
                        con.commit()
                    except Exception, e:
                        print_error("deleting entry, withdraw")
                        print_error("... failed with: %s" % (e.message))
                        con.rollback()
            else:
                continue
        except Exception, e:
            print_error("outputPostgres failed with: %s" % (e.message))
    return True

def main():
    parser = argparse.ArgumentParser(description='', epilog='')
    parser.add_argument('-l', '--logging',
                        help='Ouptut log info.', action='store_true')
    parser.add_argument('-w', '--warning',
                        help='Output warnings.', action='store_true')
    parser.add_argument('-v', '--verbose',
                        help='Verbose output.', action='store_true')
    parser.add_argument('-k', '--keepwithdrawn',
                        help='Keep withdrawn prefixes.', action='store_true')
    parser.add_argument('-d', '--database',
                        help='Postgres database connection parameters.',
                        default='dbname=lbv', type=str, required=True)

    args = vars(parser.parse_args())

    global verbose
    verbose   = args['verbose']
    global warning
    warning   = args['warning']
    global logging
    logging = args['logging']

    dbconnstr = args['database'].strip()
    global keepwithdrawn
    keepwithdrawn = args['keepwithdrawn']
    # BEGIN
    print_log(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + " starting ...")
    queue = mp.Queue()
    output_p = mp.Process(target=outputPostgres,
                           args=(dbconnstr,queue))
    output_p.start()
    # main loop
    while True:
        line = sys.stdin.readline().strip()
        try:
            data = json.loads(line)
        except:
            print_warn("Failed to parse JSON from input.")
        else:
            queue.put(data)
            print_info("output queue size: " + str(queue.qsize()))

if __name__ == "__main__":
    main()
