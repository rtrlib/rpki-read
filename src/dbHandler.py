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
from mongodb import output_data, output_stat, output_latest
from purgeNotFound import purge_notfound
from settings import *

def main():
    parser = argparse.ArgumentParser(description='', epilog='')
    parser.add_argument('-d', '--dropdata',
                        help='Drop/delete all existing data in the database.',
                        action='store_true', default=False)
    parser.add_argument('-l', '--loglevel',
                        help='Set loglevel [DEBUG,INFO,WARNING,ERROR,CRITICAL].',
                        type=str, default='WARNING')
    parser.add_argument('-m', '--mongodb',
                        help='MongoDB connection parameters.',
                        type=str, required=True)
    parser.add_argument('-p', '--purge',
                        help='Purge expired NotFound validation results.',
                        action='store_true', default=False)

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
    dbconnstr = args['mongodb'].strip()

    # thread1: write data to database
    output_data_p = mp.Process( target=output_data,
                                args=(dbconnstr,queue,args['dropdata']))
    output_data_p.start()

    # thread2: filter latest validation results
    output_latest_p = mp.Process(target=output_latest,
                                 args=(dbconnstr,))
    output_latest_p.start()

    # thread3: generate stats from database
    stats_interval = STATS_TIMEOUT
    if stats_interval < 1:
        stats_interval = 60
    output_stat_p = mp.Process( target=output_stat,
                                args=(dbconnstr,stats_interval))
    output_stat_p.start()

    # thread4: purge old validation results with NotFound [optional]
    # NOTE: if enable, you cannot recover all historic validation states
    if args['purge']:
        purge_interval = PURGE_TIMEOUT
        if purge_interval < 1:
            purge_interval = 60
        purge_p = mp.Process(   target=purge_notfound,
                                args=(dbconnstr,purge_interval))
        purge_p.start()

    # main loop, read data from STDIN to be stored in database
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
