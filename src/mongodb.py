import logging
import time

from bson.son import SON
from bson.code import Code
from datetime import datetime, timedelta
from math import sqrt
from pymongo import MongoClient, DESCENDING, ASCENDING
from settings import BULK_TIMEOUT, BULK_MAX_OPS

logging.basicConfig(level=logging.CRITICAL, format='%(asctime)s : %(levelname)s : %(message)s')

def output_latest(dbconnstr):
    """Generate and store latest validation results in database"""
    logging.info ("CALL output_latest, with mongodb: " +dbconnstr)
    # open db connection
    client = MongoClient(dbconnstr)
    db = client.get_default_database()
    f_map = Code("function() {"
                "  var key = this.prefix;"
                "  var value = {timestamp: this.timestamp, type: this.type, validated_route: null};"
                "  if (this.type == 'announcement') {"
                "    value['validated_route'] = this.validated_route;"
                "  }"
                "  emit (key, value);"
                "}")
    f_reduce = Code("function (key, values) {"
                "  var robj = { timestamp: 0, type: null, validated_route: null};"
                "  values.forEach(function(value) {"
                "    if ((value != null) && (value.timestamp > robj.timestamp)) {"
                "      robj.timestamp = value.timestamp;"
                "      robj.type = value.type;"
                "      if (value.type == 'announcement') {"
                "        robj.validated_route = value.validated_route;"
                "      }"
                "      else {"
                "        robj.validated_route = null;"
                "      }"
                "    }"
                "  } );"
                "  return robj;"
                "}")
    last_validity_count = 0
    while True:
        if "validity" in db.collection_names() and db.validity.count() != last_validity_count:
            try:
                db.validity.map_reduce( f_map,
                                        f_reduce,
                                        out=SON([("replace","validity_latest")]),
                                        )
            except Exception, e:
                logging.exception ("MapReduce failed with: " + e.message)
            else:
                last_validity_count = db.validity.count()
        # endif
        time.sleep(BULK_TIMEOUT)

def output_stat(dbconnstr, interval):
    """Generate and store validation statistics in database"""
    logging.info ("CALL output_stat, with mongodb: " +dbconnstr)
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
        if "validity_latest" in db.collection_names() and db.validity_latest.count() > 0:
            try:
                pipeline = [
                    { "$match": { 'value.type': 'announcement'} },
                    { "$group": { "_id": "$value.validated_route.validity.state", "count": { "$sum": 1} } }
                ]
                results = list(db.validity_latest.aggregate(pipeline, allowDiskUse=True ))
                for i in range(0,len(results)):
                    stats["num_"+results[i]['_id']] = results[i]['count']
                ts_tmp = db.validity_latest.find_one(projection={'value.timestamp': True, '_id': False}, sort=[('value.timestamp', -1)])['value']['timestamp']
                stats['ts'] = int(ts_tmp)
            except Exception, e:
                logging.exception ("QUERY on validity_latest failed with: " + e.message)
            # end try
        # end if
        if stats['ts'] != 'now':
            try:
                db.validity_stats.replace_one({'ts': stats['ts']}, stats, True)
            except Exception, e:
                logging.exception ("INSERT into stats failed with: " + e.message)
            # end try
        # end if
        time.sleep(interval)
    # end while

def output_data(dbconnstr, queue, dropdata):
    """Store validation results into database"""
    logging.debug ("CALL output_data mongodb, with" +dbconnstr)
    client = MongoClient(dbconnstr)
    db = client.get_default_database()
    if dropdata:
        db.validity.drop()
        db.validity_stats.drop()
        db.validity_latest.drop()
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
                cleanup_data(dbconnstr)

def cleanup_data(dbconnstr):
    """Cleanup data: remove old validation results from database"""
    logging.debug ("CALL cleanup_data mongodb, with" +dbconnstr)
    client = MongoClient(dbconnstr)
    db = client.get_default_database()
    while True:
        counter = 0
        if "validity" in db.collection_names() and db.validity.count() > 0:
            bulkRemove = db.validity.initialize_unordered_bulk_op()
            try:
                pipeline = [
                    { "$group": { "_id": '$prefix', "plist": { "$push" : { "pid": "$_id", "timestamp": "$timestamp" } }, "maxts": {"$max" : '$timestamp'} } },
                    { "$unwind": "$plist" },
                    { "$project": {"mark": { "$cond": [ { "$lt": [ "$plist.timestamp", "$maxts" ] }, "true", "false" ] }, "_id" : '$plist.pid', 'maxts': '$maxts', 'timestamp': '$plist.timestamp'} },
                    { "$match": {'mark': "true"} },
                    { "$limit": BULK_MAX_OPS}
                ]
                marked = db.validity.aggregate(pipeline, allowDiskUse=True)
                for p in marked:
                    counter += 1
                    bulkRemove.find({"_id": p['_id']}).remove_one()
                if counter > 0:
                    bulkRemove.execute()
            except Exception, e:
                logging.exception ("archive_or_purge failed with: " + e.message)
            else:
                if counter < 1:
                    break
                # end if
            # end try
        # end if
    # end while
