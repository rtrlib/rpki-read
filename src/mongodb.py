from __future__ import print_function

from pymongo import MongoClient

# internal imports
from utils import print_error, print_info, print_log, print_warn

keepwithdrawn = False

def outputMongoDB(dbconnstr, queue):
    print_log ("CALL outputMongoDB")
    client = MongoClient(dbconnstr)
    db = client['lbv']

    while True:
        data = queue.get()
        if (data == 'DONE'):
            break
        if data['type'] == 'announcement':
            print_info(".. process announcement")
            try:
                db.validity.replace_one(
                    { 'validated_route' : { 'route' : { 'prefix' : data['validated_route']['route']['prefix'] } } },
                    data, True
                )
            except Exception, e:
                print_error("updating or inserting entry, announcement")
                print_error("... failed with: %s" % (e.message))
        elif (data['type'] == 'withdraw'):
            print_info(".. process withdraw")
            if keepwithdrawn:
                try:
                    db.validity.update_one(
                        { 'validated_route' : { 'route' : { 'prefix' : data['prefix'] } } },
                        {'type': 'withdraw','validated_route.validity.state': 'withdrawn' }
                    )
                except Exception, e:
                    print_error("updating entry, withdraw")
                    print_error("... failed with: %s" % (e.message))
            else:
                try:
                    db.validity.delete_one({ 'validated_route' : { 'route' : { 'prefix' : data['prefix'] } } })
                except Exception, e:
                    print_error("deleting entry, withdraw")
                    print_error("... failed with: %s" % (e.message))
        else:
            print_warn("Type not supported, must be either announcement or withdraw!")
            continue
