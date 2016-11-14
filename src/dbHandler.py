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
                        type=str, default=DEFAULT_LOG_LEVEL)
    parser.add_argument('-m', '--mongodb',
                        help='MongoDB connection parameters.',
                        type=str, default=DEFAULT_MONGO_DATABASE['uri'])

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

    # main loop, read data from STDIN to be stored in database
    counter = 0
    if os.path.exists(WAIT_TO_SYNC_FILE):
        try:
            logging.info("remove wait_to_sync file")
            os.remove(WAIT_TO_SYNC_FILE)
        except Exception, e:
            logging.exception ("remove wait_to_sync file, failed with: %s" , e.message)
        # end try
    # end if
    while True:
        line = sys.stdin.readline().strip()
        if line.strip() == 'STOP':
            break
        # end if
        try:
            data = json.loads(line)
        except:
            logging.exception ("Failed to parse JSON from input.")
        else:
            queue.put(data)
        # end try
        counter += 1
        if counter > MAX_COUNTER:
            logging.info ("output queue size: " + str(queue.qsize()))
            counter = 0
        if (queue.qsize() > QUEUE_LIMIT) and not os.path.exists(WAIT_TO_SYNC_FILE):
            try:
                logging.info("create wait_to_sync file")
                with open(WAIT_TO_SYNC_FILE, 'a'):
                    os.utime(WAIT_TO_SYNC_FILE, None)
            except Exception, e:
                logging.exception ("create wait_to_sync file, failed with: %s" , e.message)
        elif (queue.qsize() < (QUEUE_LIMIT * 0.8)) and os.path.exists(WAIT_TO_SYNC_FILE):
            try:
                logging.info("remove wait_to_sync file")
                os.remove(WAIT_TO_SYNC_FILE)
            except Exception, e:
                logging.exception ("remove wait_to_sync file, failed with: %s" , e.message)
        # end if
    # end while

if __name__ == "__main__":
    main()
