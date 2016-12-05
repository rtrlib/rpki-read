import gc
import logging
import time

from datetime import datetime
from bson.son import SON
from bson.code import Code
from pymongo import MongoClient
from settings import BULK_TIMEOUT, BULK_MAX_OPS

logging.basicConfig(level=logging.CRITICAL, format='%(asctime)s : %(levelname)s : %(message)s')

def output_latest(dbconnstr):
    """Generate and store latest validation results in database"""
    logging.info("CALL output_latest, with mongodb: " +dbconnstr)
    # open db connection
    client = MongoClient(dbconnstr)
    database = client.get_default_database()
    f_map = Code(
        "function() {"
        "  var key = this.prefix;"
        "  var value = {timestamp: this.timestamp, type: this.type, validated_route: null};"
        "  if (this.type == 'announcement') {"
        "    value['validated_route'] = this.validated_route;"
        "  }"
        "  emit (key, value);"
        "}"
    )
    f_reduce = Code(
        "function (key, values) {"
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
        "}"
    )
    last_validity_count = 0
    while True:
        try:
            if database.validity.count() != last_validity_count:
                database.validity.map_reduce(
                    f_map, f_reduce,
                    out=SON([("replace", "validity_latest")])
                )
                last_validity_count = database.validity.count()
        except Exception as errmsg:
            logging.exception("MapReduce failed with: " + str(errmsg))
        # end try
        time.sleep(BULK_TIMEOUT)

def output_stat(dbconnstr, interval):
    """Generate and store validation statistics in database"""
    logging.info("CALL output_stat, with mongodb: " +dbconnstr)
    # open db connection
    client = MongoClient(dbconnstr)
    database = client.get_default_database()
    while True:
        # create stats
        stats = dict()
        stats['ts'] = 'now'
        stats['num_Valid'] = 0
        stats['num_InvalidAS'] = 0
        stats['num_InvalidLength'] = 0
        stats['num_NotFound'] = 0
        try:
            pipeline = [
                {"$match": {'value.type': 'announcement'}},
                {"$group": {"_id": "$value.validated_route.validity.state",
                            "count": {"$sum": 1}}}
            ]
            results = list(database.validity_latest.aggregate(pipeline, allowDiskUse=True))
            for i in range(0, len(results)):
                stats["num_"+results[i]['_id']] = results[i]['count']
            ts_tmp = database.validity_latest.find_one(
                projection={'value.timestamp': True, '_id': False},
                sort=[('value.timestamp', -1)])['value']['timestamp']
            stats['ts'] = int(ts_tmp)
        except Exception as errmsg:
            logging.exception("QUERY on validity_latest failed with: " + str(errmsg))
        # end try
        if stats['ts'] != 'now':
            try:
                database.validity_stats.replace_one({'ts': stats['ts']}, stats, True)
            except Exception as errmsg:
                logging.exception("INSERT into stats failed with: " + str(errmsg))
            # end try
        # end if
        time.sleep(interval)
    # end while

def output_data(dbconnstr, pipe, dropdata):
    """Store validation results into database"""
    logging.debug("CALL output_data mongodb, with " + dbconnstr)
    client = MongoClient(dbconnstr)
    database = client.get_default_database()
    if dropdata:
        database.validity.drop()
        database.validity_stats.drop()
        database.validity_latest.drop()
    # end dropdata
    bulk = database.validity.initialize_unordered_bulk_op()
    bulk_len = 0
    begin = datetime.now()
    while True:
        data = pipe.recv()
        if data == 'DONE':
            break
        if (data['type'] == 'announcement') or (data['type'] == 'withdraw'):
            logging.debug("process announcement or withdraw of prefix: " + data['prefix'])
            try:
                bulk.insert(data)
            except Exception as errmsg:
                logging.exception("bulk insert, failed with: " + str(errmsg))
            else:
                bulk_len += 1
        else:
            logging.warning("Type not supported, must be either announcement or withdraw!")
            continue

        now = datetime.now()
        timeout = now - begin
        # exec bulk validity
        if (bulk_len > BULK_MAX_OPS) or (timeout.total_seconds() > BULK_TIMEOUT):
            logging.info("do mongo bulk operation ...")
            # end try create file
            try:
                bulk.execute({'w': 0})
            except Exception as errmsg:
                logging.exception("bulk operation, failed with: " + str(errmsg))
            # end try bulk
            bulk = database.validity.initialize_unordered_bulk_op()
            bulk_len = 0
            cleanup_data(dbconnstr)
            gc.collect()
            begin = datetime.now()

def cleanup_data(dbconnstr):
    """Cleanup data: remove old validation results from database"""
    logging.debug("CALL cleanup_data mongodb, with " +dbconnstr)
    client = MongoClient(dbconnstr)
    database = client.get_default_database()
    counter = 0
    try:
        bulk_remove = database.validity.initialize_unordered_bulk_op()
        pipeline = [
            {"$group": {
                "_id": '$prefix',
                "plist": {"$push" : {"pid": "$_id", "timestamp": "$timestamp"}},
                "maxts": {"$max" : '$timestamp'}}
            },
            {"$unwind": "$plist"},
            {"$project": {
                "mark": {"$cond": [{"$lt": ["$plist.timestamp", "$maxts"]}, "true", "false"]},
                "_id": '$plist.pid',
                'maxts': '$maxts',
                'timestamp': '$plist.timestamp'}
            },
            {"$match": {'mark': "true"}}
        ]
        results = database.validity.aggregate(pipeline, allowDiskUse=True)
        for res in results:
            counter += 1
            bulk_remove.find({"_id": res['_id']}).remove_one()
        if counter > 0:
            bulk_remove.execute()
    except Exception as errmsg:
        logging.exception("cleanup_data failed with: " + str(errmsg))
    # end try
