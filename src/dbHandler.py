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

def main():
    parser = argparse.ArgumentParser(description='', epilog='')
    parser.add_argument('-l', '--logging',
                        help='Ouptut log info.', action='store_true')
    parser.add_argument('-w', '--warning',
                        help='Output warnings.', action='store_true')
    parser.add_argument('-v', '--verbose',
                        help='Verbose output.', action='store_true')
    parser.add_argument('-d', '--database',
                        help='Postgres database connection parameters.'
                        default='dbname=lbv', type=str, required=True)

    args = vars(parser.parse_args())

    global verbose
    verbose   = args['verbose']
    global warning
    warning   = args['warning']
    global logging
    logging = args['logging']

    dbconnstr = args['database'].strip()

    # BEGIN
    print_log(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + " starting ...")


if __name__ == "__main__":
    main()
