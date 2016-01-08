import logging
import time

from bson.son import SON
from datetime import datetime, timedelta
from math import sqrt
from pymongo import MongoClient
from settings import MAX_TIMEOUT, MAX_BULK_OPS, MAX_PURGE_ITEMS

def output_stat(dbconnstr, interval):
    """Generate and store validation statistics in database"""
    logging.debug ("CALL output_stat mongodb, with" +dbconnstr)
    # simple check if interval is valid, that is non-negative
    if interval < 1:
        logging.warning("invalid interval for output_stat, reset to 60s!")
        interval = 60
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
        try:
            if "validity" in db.collection_names() and db.validity.count() > 0:
                pipeline = [
                    { "$sort": SON( [ ( "prefix", 1), ("timestamp", -1 ) ] ) },
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
        except Exception, e:
            logging.exception ("QUERY failed with: " + e.message)
        else:
            if stats['ts'] != 'now':
                db.validity_stats.insert_one(stats)
        # purge old NotFound entries
        pipeline = [
            { "$group": { "_id": '$prefix', "plist": { "$push" : { "pid": "$_id", "timestamp": "$timestamp", "type": "$type", "validity": "$validated_route.validity.state" } }, "maxts": {"$max" : '$timestamp'} } },
            { "$unwind": "$plist" },
            { "$match": {'plist.validity' : "NotFound"} },
            { "$project": {"toDelete": { "$cond": [ { "$lt": [ "$plist.timestamp", "$maxts" ] }, "true", "false" ] }, "_id" : '$plist.pid', 'maxts': '$maxts', 'timestamp': '$plist.timestamp'} },
            { "$match": {'toDelete': "true"} },
            { "$limit": MAX_PURGE_ITEMS}
        ]
        purge = db.validity.aggregate(pipeline)
        for p in purge:
            db.validity.remove({"_id": p['_id']})
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
        if (bulk_len > MAX_BULK_OPS) or (timeout.total_seconds() > MAX_TIMEOUT):
            begin = datetime.now()
            logging.info ("do mongo bulk operation ...")
            try:
                bulk.execute({'w': 0})
            except Exception, e:
                logging.exception ("bulk operation, failed with: %s" , e.message)
            finally:
                bulk = db.validity.initialize_unordered_bulk_op()
                bulk_len = 0
