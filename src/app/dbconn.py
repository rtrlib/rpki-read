import psycopg2
import sys

from datetime import datetime
from pymongo import MongoClient

database = "mongodb"
pg_dbconnstr = "dbname=lbv host=localhost port=5432"
mg_dbconnstr = "mongodb://localhost:27017/"

def get_validation_tables():
    if database == 'postgres':
        return pg_get_validation_tables(pg_dbconnstr)

    print "NOT IMPLEMENTED YET!"
    return None

def get_validation_stats():
    if database == 'postgres':
        return pg_get_validation_stats(pg_dbconnstr)
    elif database == 'mongodb':
        return mg_get_validation_stats(mg_dbconnstr)
    print "NOT IMPLEMENTED YET!"
    return None

def mg_get_validation_stats(dbconnstr):
    client = MongoClient(dbconnstr)
    db = client['lbv']

    table = [['Validity', 'Count'], ]
    sum_all = 0
    sum_val = 0
    stats = dict()
    stats['latest_ts'] = 'now'
    num_valid = 0
    num_invalid_as = 0
    num_invalid_len = 0
    num_not_found = 0
    try:
        num_valid = db.validity.find({'validated_route.validity.state' : 'Valid' }).count()
        num_invalid_as = db.validity.find({'validated_route.validity.state' : 'InvalidAS' }).count()
        num_invalid_len = db.validity.find({'validated_route.validity.state' : 'InvalidLength' }).count()
        num_not_found = db.validity.find({'validated_route.validity.state' : 'NotFound' }).count()
        ts_tmp = db.validity.find_one(projection={'timestamp': True, '_id': False}, sort=[('timestamp', -1)])['timestamp']
        stats['latest_ts'] = datetime.fromtimestamp(int(ts_tmp)).strftime('%Y-%m-%d %H:%M:%S')
    except Exception, e:
        print "QUERY failed with"
        print e.message

    table.append(['Valid', num_valid])
    table.append(['InvalidAS', num_invalid_as])
    table.append(['InvalidLength', num_invalid_len])
    table.append(['NotFound', num_not_found])
    sum_val = num_valid + num_invalid_as + num_invalid_len
    sum_all = sum_val + num_not_found

    stats['table'] = table
    stats['sum_all'] = sum_all
    stats['sum_val'] = sum_val

    return stats

def pg_get_validation_stats(dbconnstr):
    try:
        con = psycopg2.connect(dbconnstr)
    except Exception, e:
        print "connecting to database"
        print e.message
        sys.exit(1)
    cur = con.cursor()
    counts = dict()
    table = [['Validity', 'Count'], ]
    sum_all = 0
    sum_val = 0
    stats = dict()
    stats['latest_ts'] = 'now'
    query = "SELECT state, count(*) FROM t_validity GROUP BY state"
    query_ts = "SELECT ts FROM t_validity ORDER BY ts DESC LIMIT 1"
    try:
        cur.execute(query)
        rs = cur.fetchall()
    except Exception, e:
        print "QUERY failed with"
        print e.message
        con.rollback()
    else:
        for row in rs:
            counts[row[0]] = row[1]
    try:
        cur.execute(query_ts)
        rs = cur.fetchone()
    except Exception, e:
        print "QUERY failed with"
        print e.message
        con.rollback()
    else:
        stats['latest_ts'] = rs[0].strftime('%Y-%m-%d %H:%M:%S')

    table.append(['Valid', counts.get('Valid', 0)])
    sum_val += counts.get('Valid', 0)
    table.append(['InvalidAS', counts.get('InvalidAS', 0)])
    sum_val += counts.get('InvalidAS', 0)
    table.append(['InvalidLength', counts.get('InvalidLength', 0)])
    sum_val += counts.get('InvalidLength', 0)
    table.append(['NotFound', counts.get('NotFound', 0)])
    sum_all += sum_val + counts.get('NotFound', 0)

    stats['table'] = table
    stats['sum_all'] = sum_all
    stats['sum_val'] = sum_val
    return stats

def pg_get_validation_tables(dbconnstr):
    try:
        con = psycopg2.connect(dbconnstr)
    except Exception, e:
        print "connecting to database"
        print e.message
        sys.exit(1)
    cur = con.cursor()
    query = "SELECT prefix, origin, state, ts, roas FROM t_validity WHERE state != 'NotFound' ORDER BY origin"
    tables = dict()
    tables['valid'] = list()
    tables['invalid_as'] = list()
    tables['invalid_len'] = list()
    try:
        cur.execute(query)
        rs = cur.fetchall()
    except Exception, e:
        print "QUERY failed with"
        print e.message
        con.rollback()
    else:
        for row in rs:
            if row[2] == 'Valid':
                tables['valid'].append([row[0],row[1],row[3]])
            elif row[2] == 'InvalidAS':
                tables['invalid_as'].append([row[0],row[1],row[3]])
            else:
                tables['invalid_len'].append([row[0],row[1],row[3]])

    tables['num_valid'] = len(tables['valid'])
    tables['num_invalid_as'] = len(tables['invalid_as'])
    tables['num_invalid_len'] = len(tables['invalid_len'])
    return tables
