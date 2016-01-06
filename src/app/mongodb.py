import logging

from bson.son import SON
from datetime import datetime
from pymongo import MongoClient, DESCENDING

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
    stats['stats_roa'] = []
    try:
        pipeline = [
            { "$match": { 'type' : 'announcement' } },
            { "$sort": SON( [ ( "prefix", 1), ("timestamp", -1 ) ] ) },
            { "$group": { "_id": "$prefix", "timestamp": { "$first": "$timestamp" }, "validity": { "$first": "$validated_route.validity.state"} } },
            { "$group": { "_id": "$validity", "count": { "$sum": 1} } }
        ]
        results = list(db.validity.aggregate( pipeline ))
        for i in range(0,len(results)):
            stats["num_"+results[i]['_id']] = results[i]['count']
        stats['stats_all'] = list(db.validity_stats.find({},{'_id':0}).sort('ts', DESCENDING).limit(1440))
        ts_tmp = db.validity.find_one(projection={'timestamp': True, '_id': False}, sort=[('timestamp', -1)])['timestamp']
        stats['latest_ts'] = datetime.fromtimestamp(int(ts_tmp)).strftime('%Y-%m-%d %H:%M:%S')
    except Exception, e:
        logging.exception ("QUERY failed with: " + e.message)

    return stats

def get_list(dbconnstr, state):
    client = MongoClient(dbconnstr)
    db = client.get_default_database()
    rlist = []
    try:
        pipeline = [
            { "$match" : { "$and": [ { "type": 'announcement' }, { "validated_route.validity.state": state} ] } },
            { "$sort": SON( [ ( "prefix", 1), ("timestamp", -1 ) ] ) },
            { "$group": { "_id": "$prefix", "timestamp": { "$first": "$timestamp" }, "validated_route": { "$first": "$validated_route"} } }
        ]
        rset = list(db.validity.aggregate( pipeline ))
        #rset = db.validity.find({'validated_route.validity.state' : state},{'_id' : 0, 'source' : 0, 'next_hop' : 0, 'type' : 0, 'timestamp' : 0})
    except Exception, e:
        logging.exception ("QUERY failed with: " + e.message)
    else:
        for r in rset:
            data = dict()
            data['prefix'] = r['validated_route']['route']['prefix']
            data['origin'] = r['validated_route']['route']['origin_asn']
            data['state'] = r['validated_route']['validity']['state']
            data['roas'] = r['validated_route']['validity']['VRPs']
            rlist.append(data)

    return rlist
