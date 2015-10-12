from __future__ import print_function

import psycopg2
import sys
from psycopg2.extras import Json

# internal imports
from utils import *

keepwithdrawn = False

def outputPostgres(dbconnstr, queue):
    print_info(dbconnstr)
    try:
        con = psycopg2.connect(dbconnstr)
    except Exception, e:
        print_error("failed with: %s" % (e.message))
        print_error("connecting to database")
        sys.exit(1)
    cur = con.cursor()
    update_validity =   "UPDATE t_validity SET state=%s, ts=%s, roas=%s, " \
                        "next_hop=%s,src_asn=%s, src_addr=%s WHERE prefix=%s"
    insert_validity =   "INSERT INTO t_validity (prefix, origin, state, ts, roas, next_hop, src_asn, src_addr) " \
                        "SELECT %s, %s, %s, %s, %s, %s, %s, %s " \
                        "WHERE NOT EXISTS (SELECT 1 FROM t_validity WHERE prefix=%s)"
    delete_validty =    "DELETE FROM t_validity WHERE prefix=%s"
    delete_all =        "DELETE FROM t_validity *"
    try:
        cur.execute(delete_all)
        con.commit()
    except Exception, e:
        print_error("deleting all existing entries")
        print_error("... failed with: %s" % (e.message))
        con.rollback()

    while True:
        data = queue.get()
        if (data == 'DONE'):
            break
        try:
            if data['type'] == 'announcement':
                vr = data['validated_route']
                rt = vr['route']
                vl = vr['validity']
                roas = vl['VRPs']
                src = data['source']

                ts_str = datetime.fromtimestamp(
                        int(data['timestamp'])).strftime('%Y-%m-%d %H:%M:%S')
                #print_info("converted unix timestamp: " + ts_str)
                try:
                    cur.execute(update_validity, [ vl['state'], ts_str, Json(roas),
                        data['next_hop'], src['asn'], src['addr'], rt['prefix'] ])
                    cur.execute(insert_validity, [rt['prefix'], rt['origin_asn'][2:],
                        vl['state'], ts_str, Json(roas),
                        data['next_hop'], src['asn'], src['addr'], rt['prefix']])
                    con.commit()
                except Exception, e:
                    print_error("updating or inserting entry, announcement")
                    print_error("... failed with: %s" % (e.message))
                    con.rollback()
            elif (data['type'] == 'withdraw'):
                if keepwithdrawn:
                    ts_str = datetime.fromtimestamp(
                        int(data['timestamp'])).strftime('%Y-%m-%d %H:%M:%S')
                    #print_info("converted unix timestamp: " + ts_str)
                    src = data['source']
                    try:
                        cur.execute(update_validity, ['withdrawn', ts_str, None,
                            None, src['asn'], src['addr'], data['prefix']] )
                        con.commit()
                    except Exception, e:
                        print_error("updating entry, withdraw")
                        print_error("... failed with: %s" % (e.message))
                        con.rollback()
                else:
                    try:
                        cur.execute(delete_validty, [data['prefix']])
                        con.commit()
                    except Exception, e:
                        print_error("deleting entry, withdraw")
                        print_error("... failed with: %s" % (e.message))
                        con.rollback()
            else:
                print_warn("Type not supported, must be either announcement or withdraw!")
                continue
        except Exception, e:
            print_error("outputPostgres failed with: %s" % (e.message))
            if (con.closed):
                try:
                    con = psycopg2.connect(dbconnstr)
                except Exception, e:
                    print_error("failed with: %s" % (e.message))
                    print_error("connecting to database")
                    sys.exit(1)
                cur = con.cursor()
    return True
