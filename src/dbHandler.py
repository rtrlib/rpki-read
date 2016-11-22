#!/usr/bin/python
import argparse
import gc
import json
import logging
import sys

import multiprocessing as mp

# internal imports
from mongodb import output_data, output_latest, output_stat
from settings import DEFAULT_LOG_LEVEL, DEFAULT_MONGO_DATABASE, DOSTATS_INTERVAL

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
        raise ValueError('Invalid log level: %s' % args['loglevel'])
    logging.basicConfig(level=numeric_level,
                        format='%(asctime)s : %(levelname)s : %(message)s')

    pipe_recv, pipe_send = mp.Pipe(False)
    dbconnstr = None
    # BEGIN
    logging.info("START")
    dbconnstr = args['mongodb'].strip()

    # thread1: write data to database
    output_data_p = mp.Process(target=output_data,
                               args=(dbconnstr, pipe_recv, args['dropdata']))
    output_data_p.start()

    # thread2: filter latest validation results
    output_latest_p = mp.Process(target=output_latest,
                                 args=(dbconnstr,))
    output_latest_p.start()

    # thread3: generate stats from database
    stats_interval = DOSTATS_INTERVAL
    if stats_interval < 1:
        stats_interval = 60
    output_stat_p = mp.Process(target=output_stat,
                               args=(dbconnstr, stats_interval))
    output_stat_p.start()

    # main loop, read data from STDIN to be stored in database
    while True:
        line = sys.stdin.readline().strip()
        if line.strip() == 'STOP':
            break
        # end if
        try:
            data = json.loads(line, strict=False)
        except ValueError:
            logging.exception("Failed to parse JSON from input.")
        else:
            pipe_send.send(data)
        # end try
    # end while

if __name__ == "__main__":
    main()
