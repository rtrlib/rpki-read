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
        # endif
        last_validity_count = db.validity.count()
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
                if stats['ts'] != 'now':
                    db.validity_stats.replace_one({'ts': stats['ts']}, stats, True)
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

def archive_or_purge(dbconnstr, interval, purge):
    """Archive or purge old, expired valitdation results in database"""
    logging.info ("CALL archive_or_purge, with mongodb: " +dbconnstr)
    # open db connection
    client = MongoClient(dbconnstr)
    db = client.get_default_database()
    archive_old = ""
    while(True):
        archive_str = datetime.today().strftime("archive_%Y_w%W")
        if len(archive_old) < 1:
            archive_old = archive_str
        archive_col = db[archive_str]
        counter = 0
        # archive old NotFound entries
        if "validity" in db.collection_names() and db.validity.count() > 0:
            if not purge:
                bulkInsert = archive_col.initialize_unordered_bulk_op()
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
                    if not purge:
                        bulkInsert.insert(db.validity.find_one({"_id": p['_id']}))
                    bulkRemove.find({"_id": p['_id']}).remove_one()
                if counter > 0:
                    bulkRemove.execute()
                    if not purge:
                        bulkInsert.execute()
            except Exception, e:
                logging.exception ("archive_or_purge failed with: " + e.message)
            # end try
        # end if validity
        # if archive_old != archive_str:
        #     archive_clean(dbconnstr, archive_old)
        #     archive_old = archive_str
        if counter < (BULK_MAX_OPS * 0.2):
            time.sleep(interval)
    # end while

def archive_clean(dbconnstr, archive_str):
    logging.info ("CALL archive_clean on "+archive_str+", with mongodb: " +dbconnstr)
    client = MongoClient(dbconnstr)
    db = client.get_default_database()
    if archive_str in db.collection_names() and db[archive_str].count() > 0:
        archive_col = db[archive_str]
        try:
            prefixes = archive_col.distinct('prefix')
        except Exception, e:
            logging.exception ("archive_clean, distinct failed with: " + e.message)
        else:
            for p in prefixes:
                counter = 0
                bulkRemove = archive_col.initialize_unordered_bulk_op()
                try:
                    states = archive_col.find({"prefix": p}, sort=[('timestamp', ASCENDING)])
                except Exception, e:
                    logging.exception ("archive_clean, find failed with: " + e.message)
                else:
                    s1 = None
                    for s2 in states:
                        if s1 != None:
                            logging.debug(s1)
                            logging.debug(s2)
                            if (s1['type'] != 'withdraw') and (s2['type'] != 'withdraw') and (s1['validated_route']['validity']['state'] == s2['validated_route']['validity']['state']):
                                counter += 1
                                logging.debug("remove 1")
                                bulkRemove.find({'_id': s1['_id']}).remove_one()
                            #end if
                        #end if
                        s1 = s2
                    # end for
                #end try
                if counter > 0:
                    try:
                        logging.info("archive bulkRemove for prefix: "+p)
                        bulkRemove.execute()
                    except Exception, e:
                        logging.exception ("archive_clean, bulkRemove failed with: " + e.message)
                    #end try
                #end if
            #end for
        #end try
    #end if
