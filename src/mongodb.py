from __future__ import print_function

from pymongo import MongoClient

# internal imports
from utils import print_error, print_info, print_log, print_warn

def outputMongoDB(dbconnstr, queue):
    while True:
        data = queue.get()
        if (data == 'DONE'):
            break
