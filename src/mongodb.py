import logging
import time

from datetime import datetime, timedelta
from math import sqrt
from pymongo import MongoClient
from settings import max_timeout
MAX_BULK_OPS = 987

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
            if "validity" in db.collection_names() and db.validity.count() > 0:
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
        db.validity_stats.drop()
        db.archive.drop()
    # end dropdata
    vbulk = db.validity.initialize_ordered_bulk_op()
    abulk = db.archive.initialize_ordered_bulk_op()
    bulk_len = 0
    begin = datetime.now()
    while True:
        data = queue.get()
        if (data == 'DONE'):
            break
        if data['type'] == 'announcement':
            logging.debug ("process announcement")
            try:
                logging.debug ("insert or replace prefix: " + data['validated_route']['route']['prefix'])
                vbulk.find({ 'validated_route.route.prefix' : data['validated_route']['route']['prefix'] }).upsert().replace_one(data)
            except Exception, e:
                logging.exception ("bulk update or insert entry, failed with: %s ", e.message)
            else:
                bulk_len += 1
        elif (data['type'] == 'withdraw'):
            logging.debug ("process withdraw")
            try:
                logging.debug("delete prefix if exists: " + data['prefix'] )
                vbulk.find({ 'validated_route.route.prefix' : data['prefix'] }).remove()
            except Exception, e:
                logging.exception ("bulk delete entry, failed with: %s" , e.message)
            else:
                bulk_len += 1
        else:
            logging.warning ("Type not supported, must be either announcement or withdraw!")
            continue

        # archive data?
        if keepdata:
            adata = data.copy()
            adata['archive'] = True
            logging.debug("keepdata, insert " +adata['type']+ " for prefix: " +adata['prefix'])
            try:
                abulk.insert(adata)
            except Exception, e:
                logging.exception ("archive entry, failed with: %s ", e.message)
            else:
                bulk_len += 1
        # end keepdata

        now = datetime.now()
        timeout = now - begin
        # exec bulk validity
        if (bulk_len > MAX_BULK_OPS) or (timeout.total_seconds() > max_timeout):
            begin = datetime.now()
            logging.info ("do mongo bulk operation ...")
            try:
                vbulk.execute()
                if keepdata:
                    abulk.execute()
            except Exception, e:
                logging.exception ("bulk operation, failed with: %s" , e.message)
            finally:
                vbulk = db.validity.initialize_ordered_bulk_op()
                bulk_len = 0
                if keepdata:
                    abulk = db.archive.initialize_ordered_bulk_op()
