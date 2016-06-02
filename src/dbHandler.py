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
from mongodb import *
from settings import *

def main():
    parser = argparse.ArgumentParser(description='', epilog='')
    parser.add_argument('-d', '--dropdata',
                        help='Drop/delete all existing data in the database.',
                        action='store_true', default=False)
    parser.add_argument('-l', '--loglevel',
                        help='Set loglevel [DEBUG,INFO,WARNING,ERROR,CRITICAL].',
                        type=str, default='ERROR')
    parser.add_argument('-m', '--mongodb',
                        help='MongoDB connection parameters.',
                        type=str, default=DEFAULT_MONGO_DATABASE['uri'])
    parser.add_argument('-p', '--purge',
                        help='Purge expired validation results (Default: archive).',
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
    stats_interval = DOSTATS_INTERVAL
    if stats_interval < 1:
        stats_interval = 60
    output_stat_p = mp.Process( target=output_stat,
                                args=(dbconnstr,stats_interval))
    output_stat_p.start()

    # thread4: periodically archive old validation results
    archive_interval = SERVICE_INTERVAL
    if archive_interval < 1:
        archive_interval = 300
    archive_p = mp.Process( target=archive_or_purge,
                            args=(dbconnstr, archive_interval, args['purge']))
    archive_p.start()

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
