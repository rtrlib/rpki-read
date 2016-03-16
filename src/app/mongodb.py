import logging

from bson.son import SON
from datetime import datetime
import time
from pymongo import MongoClient, DESCENDING, ASCENDING
from netaddr import *

def get_validation_stats(dbconnstr):
    client = MongoClient(dbconnstr)
    db = client.get_default_database()

    stats = dict()
    stats['latest_ts'] = 'now'
    stats['num_Valid'] = 0
    stats['num_InvalidAS'] = 0
    stats['num_InvalidLength'] = 0
    stats['num_NotFound'] = 0
    stats['stats_all'] = []
    if "validity_latest" in db.collection_names() and db.validity_latest.count() > 0:
        try:
            pipeline = [
                { "$match": { 'value.type': 'announcement'} },
                { "$group": { "_id": "$value.validated_route.validity.state", "count": { "$sum": 1} } }
            ]
            results = list(db.validity_latest.aggregate( pipeline ))
            for i in range(0,len(results)):
                stats["num_"+results[i]['_id']] = results[i]['count']
            ts24 = int(time.time()) - (3600*24) # last 24h
            stats['stats_all'] = list(db.validity_stats.find({'ts': {'$gt': ts24}},{'_id':0}).sort('ts', DESCENDING))
            ts_tmp = db.validity_latest.find_one(projection={'value.timestamp': True, '_id': False}, sort=[('value.timestamp', DESCENDING)])['value']['timestamp']
            stats['latest_ts'] = datetime.fromtimestamp(int(ts_tmp)).strftime('%Y-%m-%d %H:%M:%S')
        except Exception, e:
            logging.exception ("QUERY failed with: " + e.message)

    return stats

def get_validation_list(dbconnstr, state):
    client = MongoClient(dbconnstr)
    db = client.get_default_database()
    rlist = []
    if "validity_latest" in db.collection_names() and db.validity_latest.count() > 0:
        try:
            rset = db.validity_latest.find({'value.validated_route.validity.state' : state},{'_id' : 0, 'value.type' : 0, 'value.timestamp' : 0})
        except Exception, e:
            logging.exception ("QUERY failed with: " + e.message)
        else:
            for r in rset:
                data = dict()
                data['prefix'] = r['value']['validated_route']['route']['prefix']
                data['origin'] = r['value']['validated_route']['route']['origin_asn']
                data['state'] = r['value']['validated_route']['validity']['state']
                data['roas'] = r['value']['validated_route']['validity']['VRPs']
                rlist.append(data)

    return rlist

def get_validation_prefix(dbconnstr, search_string):
    prefix = None
    try:
        ipa = IPNetwork(search_string).ip
    except Exception, e:
        logging.exception ("IP address parse failed with: " + e.message)
    else:
        logging.info(ipa)
        client = MongoClient(dbconnstr)
        db = client.get_default_database()
        if "validity_latest" in db.collection_names() and db.validity_latest.count() > 0:
            try:
                prefixes = list(db.validity_latest.find({},{'_id': 1}))
            except Exception, e:
                logging.exception ("SEARCH failed with: " + e.message)
            else:
                for p in prefixes:
                    ipp = IPNetwork(p['_id'])
                    if ipa in ipp:
                        if prefix == None:
                            prefix = ipp
                        elif ipp.prefixlen > prefix.prefixlen:
                            prefix = ipp
        if prefix != None:
            try:
                r = list(db.validity_latest.find({'_id': str(prefix)}))[0]
                data = dict()
                data['prefix'] = r['_id']
                data['timestamp'] = r['value']['timestamp']
                data['type'] = r['value']['type']
                if data['type'] == 'announcement':
                    data['origin'] = r['value']['validated_route']['route']['origin_asn']
                    data['state'] = r['value']['validated_route']['validity']['state']
                    data['roas'] = r['value']['validated_route']['validity']['VRPs']
                else:
                    data['state'] = 'withdraw'
                prefix = list()
                prefix.append(data)
            except Exception, e:
                logging.exception ("SEARCH failed with: " + e.message)
        # end if
    return prefix

def get_validation_history(dbconnstr, search_prefix):
    rlist = list()
    client = MongoClient(dbconnstr)
    db = client.get_default_database()
    if "archive" in db.collection_names() and db.archive.count() > 0:
        try:
            rset = db.archive.find({'prefix': search_prefix}, {'_id': 0}, sort=[('timestamp', DESCENDING)])
        except Exception, e:
            logging.exception ("SEARCH failed with: " + e.message)
        else:
            for r in rset:
                data = dict()
                data['prefix'] = r['prefix']
                data['timestamp'] = r['timestamp']
                data['type'] = r['type']
                if data['type'] == 'announcement':
                    data['origin'] = r['validated_route']['route']['origin_asn']
                    data['state'] = r['validated_route']['validity']['state']
                    data['roas'] = r['validated_route']['validity']['VRPs']
                else:
                    data['state'] = 'withdraw'
                rlist.append(data)
    return rlist
