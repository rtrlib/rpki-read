import logging

from bson.son import SON
from datetime import datetime
from pymongo import MongoClient, DESCENDING, ASCENDING

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
            stats['stats_all'] = list(db.validity_stats.find({},{'_id':0}).sort('ts', DESCENDING).limit(1440))
            ts_tmp = db.validity_latest.find_one(projection={'value.timestamp': True, '_id': False}, sort=[('value.timestamp', -1)])['value']['timestamp']
            stats['latest_ts'] = datetime.fromtimestamp(int(ts_tmp)).strftime('%Y-%m-%d %H:%M:%S')
        except Exception, e:
            logging.exception ("QUERY failed with: " + e.message)

    return stats

def get_list(dbconnstr, state):
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
