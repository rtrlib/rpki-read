from __future__ import print_function

import logging
from pymongo import MongoClient

def outputMongoDB(dbconnstr, queue, dropdata):
    logging.debug ("CALL outputMongoDB")
    client = MongoClient(dbconnstr)
    db = client.get_default_database()
    if dropdata:
        db.validity.drop()
    # end dropdata
    while True:
        data = queue.get()
        if (data == 'DONE'):
            break
        if data['type'] == 'announcement':
            logging.debug ("process announcement")
            try:
                logging.debug ("insert or replace prefix: " + data['validated_route']['route']['prefix'])
                result = db.validity.replace_one(
                    { 'validated_route.route.prefix' : data['validated_route']['route']['prefix'] },
                    data, True
                )
                logging.debug("# matched: " + str(result.matched_count))
            except Exception, e:
                logging.exception ("updating or inserting entry, failed with: %s ", e.message)
        elif (data['type'] == 'withdraw'):
            logging.debug ("process withdraw")
            try:
                logging.debug("delete prefix if exists: " + data['prefix'] )
                result = db.validity.delete_one({ 'validated_route.route.prefix' : data['prefix'] })
                logging.debug("# deleted: " + str(result.deleted_count))
            except Exception, e:
                logging.exception ("deleting entry, failed with: %s" , e.message)
        else:
            logging.warning ("Type not supported, must be either announcement or withdraw!")
            continue
