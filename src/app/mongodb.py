"""
"""
import logging

from datetime import datetime
from pymongo import MongoClient, DESCENDING
from netaddr import IPNetwork

def get_ipversion_stats(dbconnstr):
    """ generate ip version specific stats from database """
    client = MongoClient(dbconnstr)
    db = client.get_default_database()
    types = ['num_', 'ips_']
    # init ipv4 stats
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
    # init ipv6 stats
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
    if ("validity_latest" in db.collection_names()) and db.validity_latest.count() > 0:
        try:
            pipeline = [{"$group": {
                "_id": '$value.validated_route.route.prefix',
                "origins": {"$push": {
                    "asn": "$value.validated_route.route.origin_asn",
                    "validity": "$value.validated_route.validity.state"}}
            }}]
            results = list(db.validity_latest.aggregate(pipeline, allowDiskUse=True))
            # parse results
            for r in results:
                if r['_id'] is None:
                    logging.debug("emtpy record, skipping")
                    continue
                ip = IPNetwork(r['_id'])
                b_val = {"Valid": False,
                         "InvalidLength": False,
                         "InvalidAS": False,
                         "NotFound": False}
                if ip.version == 4:
                    for o in r['origins']:
                        if "num_"+o['validity'] in ipv4_stats:
                            ipv4_stats["num_"+o['validity']] += 1
                        else:
                            ipv4_stats["num_"+o['validity']] = 1
                        b_val[o['validity']] = True
                    if b_val['Valid']:
                        ipv4_stats["ips_Valid"] += ip.size
                        ipv4_stats["pfx_Valid"].append(ip.prefixlen)
                    elif b_val['InvalidLength']:
                        ipv4_stats["ips_InvalidLength"] += ip.size
                        ipv4_stats["pfx_InvalidLength"].append(ip.prefixlen)
                    elif b_val['InvalidAS']:
                        ipv4_stats["ips_InvalidAS"] += ip.size
                        ipv4_stats["pfx_InvalidAS"].append(ip.prefixlen)
                    elif b_val['NotFound']:
                        ipv4_stats["ips_NotFound"] += ip.size
                        ipv4_stats["pfx_NotFound"].append(ip.prefixlen)
                elif ip.version == 6:
                    for o in r['origins']:
                        if "num_"+o['validity'] in ipv6_stats:
                            ipv6_stats["num_"+o['validity']] += 1
                        else:
                            ipv6_stats["num_"+o['validity']] = 1
                        b_val[o['validity']] = True
                    if b_val['Valid']:
                        ipv6_stats["ips_Valid"] += ip.size
                        ipv6_stats["pfx_Valid"].append(ip.prefixlen)
                    elif b_val['InvalidLength']:
                        ipv6_stats["ips_InvalidLength"] += ip.size
                        ipv6_stats["pfx_InvalidLength"].append(ip.prefixlen)
                    elif b_val['InvalidAS']:
                        ipv6_stats["ips_InvalidAS"] += ip.size
                        ipv6_stats["pfx_InvalidAS"].append(ip.prefixlen)
                    elif b_val['NotFound']:
                        ipv6_stats["ips_NotFound"] += ip.size
                        ipv6_stats["pfx_NotFound"].append(ip.prefixlen)
                    # end if b_val
                # end if ip.version
            # end for results
        except Exception as errmsg:
            logging.exception("get_ipversion_stats, error: " + str(errmsg))
            ipv4_stats = None
            ipv6_stats = None
        # end try
    # end if
    return ipv4_stats, ipv6_stats

def get_dash_stats(dbconnstr):
    client = MongoClient(dbconnstr)
    db = client.get_default_database()
    # init stats results
    stats = dict()
    stats['latest_dt'] = 'now'
    stats['latest_ts'] = 0
    stats['num_Valid'] = 0
    stats['num_InvalidAS'] = 0
    stats['num_InvalidLength'] = 0
    stats['num_NotFound'] = 0
    stats['num_Total'] = 0
    if "validity_latest" in db.collection_names() and db.validity_latest.count() > 0:
        try:
            pipeline = [
                {"$match": {'value.type': 'announcement'}},
                {"$group": {"_id": "$value.validated_route.validity.state", "count": {"$sum": 1}}}
            ]
            results = list(db.validity_latest.aggregate(pipeline, allowDiskUse=True))
            for i in range(0, len(results)):
                stats["num_"+results[i]['_id']] = results[i]['count']
                stats['num_Total'] += results[i]['count']
            stats['latest_ts'] = db.validity_latest.find_one(projection={'value.timestamp': True, '_id': False},
                                                             sort=[('value.timestamp', DESCENDING)])['value']['timestamp']
            stats['latest_dt'] = datetime.fromtimestamp(int(stats['latest_ts'])).strftime('%Y-%m-%d %H:%M:%S')
        except Exception as errmsg:
            logging.exception("get_dash_stats, error: " + str(errmsg))
            stats = None
        # end try
    # end if
    return stats

def get_last24h_stats(dbconnstr, latest_ts):
    client = MongoClient(dbconnstr)
    db = client.get_default_database()

    last24h = None
    if "validity_stats" in db.collection_names() and db.validity_stats.count() > 0:
        try:
            ts24 = int(latest_ts) - (3600*24) # last 24h
            last24h = list(db.validity_stats.find({'ts': {'$gt': ts24}},
                                                  {'_id':0}).sort('ts', DESCENDING))
        except Exception as errmsg:
            logging.exception("get_last24h_stats, error: " + str(errmsg))
            last24h = None
        # end try
    # end if
    return last24h

def get_validation_list(dbconnstr, state):
    client = MongoClient(dbconnstr)
    db = client.get_default_database()
    rlist = []
    if "validity_latest" in db.collection_names() and db.validity_latest.count() > 0:
        try:
            rset = db.validity_latest.find({'value.validated_route.validity.state' : state},
                                           {'_id' : 0, 'value.type' : 0, 'value.timestamp' : 0})
            for r in rset:
                data = dict()
                data['prefix'] = r['value']['validated_route']['route']['prefix']
                data['origin'] = r['value']['validated_route']['route']['origin_asn']
                data['state'] = r['value']['validated_route']['validity']['state']
                data['roas'] = r['value']['validated_route']['validity']['VRPs']
                rlist.append(data)
        except Exception as errmsg:
            logging.exception("get_validation_list, error: " + str(errmsg))
    return rlist

def get_validation_origin(dbconnstr, search_string):
    result = None
    tmp_list = None
    client = MongoClient(dbconnstr)
    db = client.get_default_database()
    if "validity_latest" in db.collection_names() and db.validity_latest.count() > 0:
        try:
            pipeline = [
                {"$match": {'value.validated_route.route.origin_asn': search_string}}
            ]
#            tmp_list = db.validity_latest.find({'value.validated_route.route.origin_asn' : search_string},
#                                           {'_id' : 0, 'value.type' : 0, 'value.timestamp' : 0})
            tmp_list = list(db.validity_latest.aggregate(pipeline, allowDiskUse=True))
        except Exception as errmsg:
            logging.exception("get_validation_origin failed with: " + str(errmsg))
        else:
            result = list()
            for r in tmp_list:
                data = dict()
                data['prefix'] = r['value']['validated_route']['route']['prefix']
                data['origin'] = r['value']['validated_route']['route']['origin_asn']
                if r['value']['type'] == 'announcement':
                    data['state'] = r['value']['validated_route']['validity']['state']
                    data['roas'] = r['value']['validated_route']['validity']['VRPs']
                else:
                    data['state'] = 'withdraw'
                    data['roas'] = None
                result.append(data)
        # end try
    # end if
    return result

def get_validation_prefix(dbconnstr, search_string):
    prefix = None
    result = None
    try:
        ipa = IPNetwork(search_string).ip
    except Exception as errmsg:
        logging.exception("IP address parse failed with: " + str(errmsg))
    else:
        client = MongoClient(dbconnstr)
        db = client.get_default_database()
        while prefix is None:
            if "validity_latest" in db.collection_names() and db.validity_latest.count() > 0:
                try:
                    prefixes = list(db.validity_latest.find({}, {'_id': 1}))
                    prefix = IPNetwork("0.0.0.0/0")
                    for p in prefixes:
                        ipp = IPNetwork(p['_id'])
                        if (ipa in ipp) and (ipp.prefixlen > prefix.prefixlen):
                            prefix = ipp
                except Exception as errmsg:
                    logging.exception("SEARCH failed with: " + str(errmsg))
                    prefix = None
                # end try
            # end if
        # end while
        try:
            ret = list(db.validity_latest.find({'_id': str(prefix)}))
            result = list()
            for r in ret:
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
                result.append(data)
        except Exception as errmsg:
            logging.exception("SEARCH failed with: " + str(errmsg))
            result = None
        # end try
    return result

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
        except Exception as errmsg:
            logging.exception("SEARCH failed with: " + str(errmsg))
    return rlist
