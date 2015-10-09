import psycopg2
import sys

from datetime import datetime

database = "postgres"
pg_dbconnstr = "dbname=lbv host=localhost port=5432"

def get_validation_table():
    if database == 'postgres':
        return pg_get_validation_table(pg_dbconnstr)

    print "NOT IMPLEMENTED YET!"
    return None

def get_validation_stats():
    if database == 'postgres':
        return pg_get_validation_stats(pg_dbconnstr)

    print "NOT IMPLEMENTED YET!"
    return None

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

def pg_get_validation_table(dbconnstr):
    try:
        con = psycopg2.connect(dbconnstr)
    except Exception, e:
        print "connecting to database"
        print e.message
        sys.exit(1)
    cur = con.cursor()
