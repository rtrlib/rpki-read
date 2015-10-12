from __future__ import print_function

import logging
from pymongo import MongoClient

def outputMongoDB(dbconnstr, queue):
    logging.debug ("CALL outputMongoDB")
    client = MongoClient(dbconnstr)
    db = client['lbv']

    while True:
        data = queue.get()
        if (data == 'DONE'):
            break
        if data['type'] == 'announcement':
            logging.info (".. process announcement")
            try:
                db.validity.replace_one(
                    { 'validated_route' : { 'route' : { 'prefix' : data['validated_route']['route']['prefix'] } } },
                    data, True
                )
            except Exception, e:
                logging.exception ("updating or inserting entry, failed with: %s ", e.message)
        elif (data['type'] == 'withdraw'):
            logging.info (".. process withdraw")
            try:
                db.validity.delete_one({ 'validated_route' : { 'route' : { 'prefix' : data['prefix'] } } })
            except Exception, e:
                logging.exception ("deleting entry, failed with: %s" , e.message)
        else:
            logging.warning ("Type not supported, must be either announcement or withdraw!")
            continue
