import logging

from bson.son import SON
from datetime import datetime
import time
from pymongo import MongoClient, DESCENDING, ASCENDING
from netaddr import *

logging.basicConfig(level=logging.CRITICAL, format='%(asctime)s : %(levelname)s : %(message)s')

def get_ipversion_stats(dbconnstr):
    print "get_ipversion_stats"
    client = MongoClient(dbconnstr)
    db = client.get_default_database()
    types = ['origins_', 'ips_']
    ipv4_stats = dict()
    for t in types:
        ipv4_stats[t+'Valid'] = 0
        ipv4_stats[t+'InvalidAS'] = 0
        ipv4_stats[t+'InvalidLength'] = 0
        ipv4_stats[t+'NotFound'] = 0
    ipv4_stats['pfx_Valid'] = []
    ipv4_stats['pfx_InvalidAS'] = []
    ipv4_stats['pfx_InvalidLength'] = []
    ipv4_stats['pfx_NotFound'] = []
    ipv6_stats = dict()
    for t in types:
        ipv6_stats[t+'Valid'] = 0
        ipv6_stats[t+'InvalidAS'] = 0
        ipv6_stats[t+'InvalidLength'] = 0
        ipv6_stats[t+'NotFound'] = 0
    ipv6_stats['pfx_Valid'] = []
    ipv6_stats['pfx_InvalidAS'] = []
    ipv6_stats['pfx_InvalidLength'] = []
    ipv6_stats['pfx_NotFound'] = []
    if "validity_latest" in db.collection_names() and db.validity_latest.count() > 0:
        try:
            pipeline = [ {
                "$group": {
                    "_id": '$value.validated_route.route.prefix',
                    "origins": {
                        "$push" : {
                            "asn": "$value.validated_route.route.origin_asn",
                            "validity": "$value.validated_route.validity.state"
                        }
                    }
                },
            } ]
            results = list(db.validity_latest.aggregate(pipeline, allowDiskUse=True))
            # parse results
            for r in results:
                if r['_id'] == None:
                    logging.debug("emtpy record, skipping")
                    continue
                logging.debug(str(r))
                ip = IPNetwork(r['_id'])
                b_val = {"Valid": False, "InvalidLength": False, "InvalidAS": False, "NotFound": False}
                if ip.version == 4:
                    for o in r['origins']:
                        ipv4_stats["origins_"+o['validity']] += 1
                        b_val[o['validity']] = True
                    if b_val['Valid'] == True:
                        ipv4_stats["ips_Valid"] += ip.size
                        ipv4_stats["pfx_Valid"].append(ip.prefixlen)
                    elif b_val['InvalidLength'] == True:
                        ipv4_stats["ips_InvalidLength"] += ip.size
                        ipv4_stats["pfx_InvalidLength"].append(ip.prefixlen)
                    elif b_val['InvalidAS'] == True:
                        ipv4_stats["ips_InvalidAS"] += ip.size
                        ipv4_stats["pfx_InvalidAS"].append(ip.prefixlen)
                    elif b_val['NotFound'] == True:
                        ipv4_stats["ips_NotFound"] += ip.size
                        ipv4_stats["pfx_NotFound"].append(ip.prefixlen)
                elif ip.version == 6:
                    for o in r['origins']:
                        ipv6_stats["origins_"+o['validity']] += 1
                        b_val[o['validity']] = True
                    if b_val['Valid'] == True:
                        ipv6_stats["ips_Valid"] += ip.size
                        ipv6_stats["pfx_Valid"].append(ip.prefixlen)
                    elif b_val['InvalidLength'] == True:
                        ipv6_stats["ips_InvalidLength"] += ip.size
                        ipv6_stats["pfx_InvalidLength"].append(ip.prefixlen)
                    elif b_val['InvalidAS'] == True:
                        ipv6_stats["ips_InvalidAS"] += ip.size
                        ipv6_stats["pfx_InvalidAS"].append(ip.prefixlen)
                    elif b_val['NotFound'] == True:
                        ipv6_stats["ips_NotFound"] += ip.size
                        ipv6_stats["pfx_NotFound"].append(ip.prefixlen)
        except Exception, e:
            logging.exception ("QUERY failed with: " + e.message)
            print "get_ipversion_stats: error"
            ipv4_stats = None
            ipv6_stats = None
        # end try
    # end if
    return ipv4_stats, ipv6_stats

def get_latest_stats(dbconnstr):
    client = MongoClient(dbconnstr)
    db = client.get_default_database()
    # init stats results
    stats = dict()
    stats['latest_ts'] = 'now'
    stats['num_Valid'] = 0
    stats['num_InvalidAS'] = 0
    stats['num_InvalidLength'] = 0
    stats['num_NotFound'] = 0
    if "validity_latest" in db.collection_names() and db.validity_latest.count() > 0:
        try:
            pipeline = [
                { "$match": { 'value.type': 'announcement'} },
                { "$group": { "_id": "$value.validated_route.validity.state", "count": { "$sum": 1 } } }
            ]
            results = list(db.validity_latest.aggregate(pipeline, allowDiskUse=True ))
            for i in range(0,len(results)):
                stats["num_"+results[i]['_id']] = results[i]['count']
            ts_tmp = db.validity_latest.find_one(projection={'value.timestamp': True, '_id': False}, sort=[('value.timestamp', DESCENDING)])['value']['timestamp']
            stats['latest_ts'] = datetime.fromtimestamp(int(ts_tmp)).strftime('%Y-%m-%d %H:%M:%S')
        except Exception, e:
            logging.exception ("QUERY failed with: " + e.message)
            stats = None
        # end try
    # end if
    return stats

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
                { "$group": { "_id": "$value.validated_route.validity.state", "count": { "$sum": 1 } } }
            ]
            results = list(db.validity_latest.aggregate(pipeline, allowDiskUse=True ))
            for i in range(0,len(results)):
                stats["num_"+results[i]['_id']] = results[i]['count']
            ts24 = int(time.time()) - (3600*24) # last 24h
            stats['stats_all'] = list(db.validity_stats.find({'ts': {'$gt': ts24}},{'_id':0}).sort('ts', DESCENDING))
            ts_tmp = db.validity_latest.find_one(projection={'value.timestamp': True, '_id': False}, sort=[('value.timestamp', DESCENDING)])['value']['timestamp']
            stats['latest_ts'] = datetime.fromtimestamp(int(ts_tmp)).strftime('%Y-%m-%d %H:%M:%S')
        except Exception, e:
            logging.exception ("QUERY failed with: " + e.message)
            stats = None
        # end try
    # end if
    return stats

def get_validation_list(dbconnstr, state):
    client = MongoClient(dbconnstr)
    db = client.get_default_database()
    rlist = []
    if "validity_latest" in db.collection_names() and db.validity_latest.count() > 0:
        try:
            rset = db.validity_latest.find({'value.validated_route.validity.state' : state},{'_id' : 0, 'value.type' : 0, 'value.timestamp' : 0})
            for r in rset:
                data = dict()
                data['prefix'] = r['value']['validated_route']['route']['prefix']
                data['origin'] = r['value']['validated_route']['route']['origin_asn']
                data['state'] = r['value']['validated_route']['validity']['state']
                data['roas'] = r['value']['validated_route']['validity']['VRPs']
                rlist.append(data)
        except Exception, e:
            logging.exception ("QUERY failed with: " + e.message)
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
                for p in prefixes:
                    ipp = IPNetwork(p['_id'])
                    if ipa in ipp:
                        if prefix == None:
                            prefix = ipp
                        elif ipp.prefixlen > prefix.prefixlen:
                            prefix = ipp
            except Exception, e:
                logging.exception ("SEARCH failed with: " + e.message)
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
        except Exception, e:
            logging.exception ("SEARCH failed with: " + e.message)
    return rlist
