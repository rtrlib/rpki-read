import logging
import time

from bson.son import SON
from datetime import datetime, timedelta
from math import sqrt
from pymongo import MongoClient, DESCENDING, ASCENDING
from settings import BULK_TIMEOUT, BULK_MAX_OPS

def output_stat(dbconnstr, interval):
    """Generate and store validation statistics in database"""
    logging.debug ("CALL output_stat, with mongodb: " +dbconnstr)
    # open db connection
    client = MongoClient(dbconnstr)
    db = client.get_default_database()
    while True:
        # create stats
        stats = dict()
        stats['ts'] = 'now'
        stats['num_Valid'] = 0
        stats['num_InvalidAS'] = 0
        stats['num_InvalidLength'] = 0
        stats['num_NotFound'] = 0
        if "validity" in db.collection_names() and db.validity.count() > 0:
            try:
                pipeline = [
                    { "$sort": SON( [ ( "prefix", ASCENDING), ("timestamp", DESCENDING ) ] ) },
                    { "$group": {   "_id": "$prefix",
                                    "timestamp": { "$first": "$timestamp" },
                                    "validity": { "$first": "$validated_route.validity.state"},
                                    "type" : { "$first" : "$type" }
                                }
                    },
                    { "$match": { 'type' : 'announcement' } },
                    { "$group": { "_id": "$validity", "count": { "$sum": 1} } }
                ]
                results = list(db.validity.aggregate( pipeline ))
                for i in range(0,len(results)):
                    stats["num_"+results[i]['_id']] = results[i]['count']
                ts_tmp = db.validity.find_one(projection={'timestamp': True, '_id': False}, sort=[('timestamp', -1)])['timestamp']
                stats['ts'] = int(ts_tmp)
                if stats['ts'] != 'now':
                    db.validity_stats.insert_one(stats)
            except Exception, e:
                logging.exception ("QUERY failed with: " + e.message)

        time.sleep(interval)

def output_data(dbconnstr, queue, dropdata):
    """Store validation results into database"""
    logging.debug ("CALL output_data mongodb, with" +dbconnstr)
    client = MongoClient(dbconnstr)
    db = client.get_default_database()
    if dropdata:
        db.validity.drop()
        db.validity_stats.drop()
    # end dropdata
    bulk = db.validity.initialize_unordered_bulk_op()
    bulk_len = 0
    begin = datetime.now()
    while True:
        data = queue.get()
        if (data == 'DONE'):
            break
        if (data['type'] == 'announcement') or (data['type'] == 'withdraw'):
            logging.debug ("process announcement or withdraw of prefix: " + data['prefix'])
            try:
                bulk.insert(data)
            except Exception, e:
                logging.exception ("bulk insert, failed with: %s ", e.message)
            else:
                bulk_len += 1
        else:
            logging.warning ("Type not supported, must be either announcement or withdraw!")
            continue

        now = datetime.now()
        timeout = now - begin
        # exec bulk validity
        if (bulk_len > BULK_MAX_OPS) or (timeout.total_seconds() > BULK_TIMEOUT):
            begin = datetime.now()
            logging.info ("do mongo bulk operation ...")
            try:
                bulk.execute({'w': 0})
            except Exception, e:
                logging.exception ("bulk operation, failed with: %s" , e.message)
            finally:
                bulk = db.validity.initialize_unordered_bulk_op()
                bulk_len = 0
