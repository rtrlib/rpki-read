import logging
import time

from datetime import datetime
from pymongo import MongoClient

def output_stat(dbconnstr, interval):
    logging.debug ("CALL output_stat mongodb, with" +dbconnstr)
    if interval < 1:
        logging.warning("invalid interval for output_stat, reset to 60s!")
        interval = 60

    client = MongoClient(dbconnstr)
    db = client.get_default_database()

    while True:
        stats = dict()
        stats['ts'] = 'now'
        stats['num_valid'] = 0
        stats['num_invalid_as'] = 0
        stats['num_invalid_len'] = 0
        stats['num_not_found'] = 0
        try:
            stats['num_valid'] = db.validity.find({'validated_route.validity.state' : 'Valid' }).count()
            stats['num_invalid_as'] = db.validity.find({'validated_route.validity.state' : 'InvalidAS' }).count()
            stats['num_invalid_len'] = db.validity.find({'validated_route.validity.state' : 'InvalidLength' }).count()
            stats['num_not_found'] = db.validity.find({'validated_route.validity.state' : 'NotFound' }).count()
            ts_tmp = db.validity.find_one(projection={'timestamp': True, '_id': False}, sort=[('timestamp', -1)])['timestamp']
            stats['ts'] = int(ts_tmp)
        except Exception, e:
            logging.exception ("QUERY failed with: " + e.message)
        else:
            if stats['ts'] != 'now':
                db.validity_stats.insert_one(stats)
        time.sleep(interval)

def output_data(dbconnstr, queue, dropdata, keepdata):
    logging.debug ("CALL output_data mongodb, with" +dbconnstr)
    client = MongoClient(dbconnstr)
    db = client.get_default_database()
    if dropdata:
        db.validity.drop()
    # end dropdata
    while True:
        data = queue.get()
        if (data == 'DONE'):
            break
        if keepdata:
            logging.debug("keepdata, insert " +data['type']+ " prefix: " +data['prefix'])
            try:
                result = db.archive.insert_one(data).inserted_id
                logging.debug ("inserted_id: " +str(result))
            except Exception, e:
                logging.exception ("insert entry, failed with: %s ", e.message)
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
                logging.exception ("update or insert entry, failed with: %s ", e.message)
        elif (data['type'] == 'withdraw'):
            logging.debug ("process withdraw")
            try:
                logging.debug("delete prefix if exists: " + data['prefix'] )
                result = db.validity.delete_one({ 'validated_route.route.prefix' : data['prefix'] })
                logging.debug("# deleted: " + str(result.deleted_count))
            except Exception, e:
                logging.exception ("delete entry, failed with: %s" , e.message)
        else:
            logging.warning ("Type not supported, must be either announcement or withdraw!")
            continue
