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
from psycopg2.extras import Json

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
    update_validity =   "UPDATE t_validity SET state=%s, ts=%s, roas=%s, " \
                        "next_hop=%s,src_asn=%s, src_addr=%s WHERE prefix=%s"
    insert_validity =   "INSERT INTO t_validity (prefix, origin, state, ts, roas, next_hop, src_asn, src_addr) " \
                        "SELECT %s, %s, %s, %s, %s, %s, %s, %s " \
                        "WHERE NOT EXISTS (SELECT 1 FROM t_validity WHERE prefix=%s)"
    delete_validty =    "DELETE FROM t_validity WHERE prefix=%s"
    delete_all =        "DELETE FROM t_validity *"
    try:
        cur.execute(delete_all)
        con.commit()
    except Exception, e:
        print_error("deleting all existing entries")
        print_error("... failed with: %s" % (e.message))
        con.rollback()

    while True:
        data = queue.get()
        if (data == 'DONE'):
            break
        try:
            if data['type'] == 'announcement':
                vr = data['validated_route']
                rt = vr['route']
                vl = vr['validity']
                roas = vl['VRPs']
                src = data['source']

                ts_str = datetime.fromtimestamp(
                        int(data['timestamp'])).strftime('%Y-%m-%d %H:%M:%S')
                #print_info("converted unix timestamp: " + ts_str)
                try:
                    cur.execute(update_validity, [ vl['state'], ts_str, Json(roas),
                        data['next_hop'], src['asn'], src['addr'], rt['prefix'] ])
                    cur.execute(insert_validity, [rt['prefix'], rt['origin_asn'][2:],
                        vl['state'], ts_str, Json(roas),
                        data['next_hop'], src['asn'], src['addr'], rt['prefix']])
                    con.commit()
                except Exception, e:
                    print_error("updating or inserting entry, announcement")
                    print_error("... failed with: %s" % (e.message))
                    con.rollback()
            elif (data['type'] == 'withdraw'):
                if keepwithdrawn:
                    ts_str = datetime.fromtimestamp(
                        int(data['timestamp'])).strftime('%Y-%m-%d %H:%M:%S')
                    #print_info("converted unix timestamp: " + ts_str)
                    src = data['source']
                    try:
                        cur.execute(update_validity, ['withdrawn', ts_str, None,
                            None, src['asn'], src['addr'], data['prefix']] )
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
            if (con.closed):
                try:
                    con = psycopg2.connect(dbconnstr)
                except Exception, e:
                    print_error("failed with: %s" % (e.message))
                    print_error("connecting to database")
                    sys.exit(1)
                cur = con.cursor()
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
    db = parser.add_mutually_exclusive_group(required=True)
    db.add_argument(    '-c', '--couchdb',
                        help='CouchDB connection parameters.',
                        default=False)
    db.add_argument(    '-m', '--mongodb',
                        help='MongoDB connection parameters.',
                        default=False)
    db.add_argument(    '-p', '--postgres',
                        help='PostgreSQL connection parameters.',
                        default=False)

    args = vars(parser.parse_args())

    global verbose
    verbose   = args['verbose']
    global warning
    warning   = args['warning']
    global logging
    logging = args['logging']
    global keepwithdrawn
    keepwithdrawn = args['keepwithdrawn']

    queue = mp.Queue()
    # BEGIN
    print_log(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + " starting ...")
    if args['couchdb']:
        print_error("Support for CouchDB not implemented yet!")
        sys.exit(1)
    if args['mongodb']:
        print_error("Support for MongoDB not implemented yet!")
        sys.exit(1)
    if args['postgres']:
        dbconnstr = args['postgres'].strip()
        output_p = mp.Process(  target=outputPostgres,
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
            if (queue.qsize() > 100000):
                print_warn("output queue size exceeds threshold, restart output thread!")
                output_p.terminate()
                output_p.start()

if __name__ == "__main__":
    main()
