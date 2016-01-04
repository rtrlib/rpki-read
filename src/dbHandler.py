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

def main():
    parser = argparse.ArgumentParser(description='', epilog='')
    parser.add_argument('-d', '--dropdata',
                        help='Drop/delete all existing data in the database.',
                        action='store_true', default=False)
    parser.add_argument('-k', '--keepdata',
                        help='Keep all data, never replace anything.',
                        action='store_true', default=False)
    parser.add_argument('-l', '--loglevel',
                        help='Set loglevel [DEBUG,INFO,WARNING,ERROR,CRITICAL].',
                        type=str, default='WARNING')
    parser.add_argument('-i', '--interval',
                        help='Set timeout interval for stats, default: 60s.',
                        type=int, default=60)
    db = parser.add_mutually_exclusive_group(required=True)
    db.add_argument(    '-m', '--mongodb',
                        help='MongoDB connection parameters.',
                        default=False)
    db.add_argument(    '-p', '--postgres',
                        help='PostgreSQL connection parameters.',
                        default=False)

    args = vars(parser.parse_args())

    numeric_level = getattr(logging, args['loglevel'].upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % loglevel)
    logging.basicConfig(level=numeric_level,
                        format='%(asctime)s : %(levelname)s : %(message)s')

    queue = mp.Queue()
    dbconnstr = None
    # BEGIN
    logging.info("START")
    if args['mongodb']:
        logging.info("database: MongoDB")
        from mongodb import output_data, output_stat
        dbconnstr = args['mongodb'].strip()

    elif args['postgres']:
        logging.info("database: PostgreSQL")
        from postgresql import output_data, output_stat
        dbconnstr = args['postgres'].strip()

    output_data_p = mp.Process( target=output_data,
                                args=(dbconnstr,queue,args['dropdata'],args['keepdata']))
    output_data_p.start()

    output_stat_p = mp.Process( target=output_stat,
                                args=(dbconnstr,args['interval']))
    output_stat_p.start()
    # main loop
    counter = 0
    while True:
        line = sys.stdin.readline().strip()
        try:
            data = json.loads(line)
        except:
            logging.exception ("Failed to parse JSON from input.")
        else:
            queue.put(data)
        counter += 1
        if counter > MAX_COUNTER:
            logging.info ("output queue size: " + str(queue.qsize()))
            counter = 0

if __name__ == "__main__":
    main()
