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

import multiprocessing as mp

from datetime import datetime
from subprocess import PIPE, Popen

# internal imports
from settings import *
from utils import print_error, print_info, print_log, print_warn

keepwithdrawn = False

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
    elif args['mongodb']:
        from pymongo import MongoClient
        from mongodb import outputMongoDB
        dbconnstr = args['mongodb'].strip()
        output_p = mp.Process(  target=outputMongoDB,
                                args=(dbconnstr,queue))
    elif args['postgres']:
        import psycopg2
        from psycopg2.extras import Json
        from postgresql import outputPostgres
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
