import codecs
import json
import logging
import markdown
import sys
import socket

from apscheduler.schedulers.background import BackgroundScheduler
from flask import render_template, Markup, request
from app import app

import config
if config.DATABASE_TYPE == 'mongodb':
    from mongodb import *
else:
    logging.critical("unknown database type!")
    sys.exit(1)

logging.basicConfig(level=logging.CRITICAL, format='%(asctime)s : %(levelname)s : %(message)s')

g_dash_stats = dict()
g_ipv4_stats = dict()
g_ipv6_stats = dict()

def update_validation_stats():
    #app.logging.debug("update_validation_stats")
    dash_stats = get_validation_stats(config.DATABASE_CONN)
    ipv4_stats, ipv6_stats = get_ipversion_stats(config.DATABASE_CONN)
    global g_dash_stats, g_ipv4_stats, g_ipv6_stats
    if dash_stats != None:
        g_dash_stats = dash_stats
    if ipv4_stats != None:
        g_ipv4_stats = ipv4_stats
    if ipv6_stats != None:
        g_ipv6_stats = ipv6_stats

@app.before_first_request
def initialize():
    apsched = BackgroundScheduler()
    update_validation_stats()
    apsched.add_job(update_validation_stats, 'interval', seconds=config.UPDATE_INTERVAL_STATS)
    apsched.start()

def _get_table_json(state):
    dlist = get_validation_list(config.DATABASE_CONN, state)
    data = dict()
    data['total'] = len(dlist)
    data['state'] = state
    data['rows'] = dlist
    return json.dumps(dlist, indent=2, separators=(',', ': '))

## about page handler
@app.route('/about')
def about():
    md_file = codecs.open("../README.md", mode="r", encoding="utf-8")
    md_text = md_file.read()
    content = Markup(markdown.markdown(md_text))
    return render_template("about.html", content=content)

## dashboard handler
@app.route('/')
@app.route('/dashboard')
@app.route('/search', methods=['GET'])
def dashboard():
    dash_stats = g_dash_stats.copy()
    table = [['Validity', 'Count']]
    table.append([ 'Valid', dash_stats['num_Valid'] ])
    table.append([ 'Invalid Length', dash_stats['num_InvalidLength'] ])
    table.append([ 'Invalid AS', dash_stats['num_InvalidAS'] ])
    dash_stats['table_roa'] = table
    table_all = list(table)
    table_all.append([ 'Not Found', dash_stats['num_NotFound'] ])
    dash_stats['table_all'] = table_all
    dash_stats['source'] = config.BGPMON_SOURCE
    return render_template("dashboard.html", stats=dash_stats)

## stats handler
@app.route('/stats')
def stats():
    stats=dict()
    try:
        # ipv4 origin stats
        table = [['Validity', 'Count']]
        table.append([ 'Valid', g_ipv4_stats['origins_Valid'] ])
        table.append([ 'Invalid Length', g_ipv4_stats['origins_InvalidLength'] ])
        table.append([ 'Invalid AS', g_ipv4_stats['origins_InvalidAS'] ])
        table.append([ 'Not Found', g_ipv4_stats['origins_NotFound'] ])
        stats['ipv4_origins'] = table
        # ipv4 space coverage stats
        table = [['Validity', 'Count']]
        table.append([ 'Valid', g_ipv4_stats['ips_Valid'] ])
        table.append([ 'Invalid Length', g_ipv4_stats['ips_InvalidLength'] ])
        table.append([ 'Invalid AS', g_ipv4_stats['ips_InvalidAS'] ])
        table.append([ 'Not Found', g_ipv4_stats['ips_NotFound'] ])
        stats['ipv4_coverage'] = table
        # ipv6 origin stats
        table = [['Validity', 'Count']]
        table.append([ 'Valid', g_ipv6_stats['origins_Valid'] ])
        table.append([ 'Invalid Length', g_ipv6_stats['origins_InvalidLength'] ])
        table.append([ 'Invalid AS', g_ipv6_stats['origins_InvalidAS'] ])
        table.append([ 'Not Found', g_ipv6_stats['origins_NotFound'] ])
        stats['ipv6_origins'] = table
        # ipv6 space coverage stats
        table = [['Validity', 'Count']]
        table.append([ 'Valid', g_ipv6_stats['ips_Valid'] ])
        table.append([ 'Invalid Length', g_ipv6_stats['ips_InvalidLength'] ])
        table.append([ 'Invalid AS', g_ipv6_stats['ips_InvalidAS'] ])
        table.append([ 'Not Found', g_ipv6_stats['ips_NotFound'] ])
        stats['ipv6_coverage'] = table
        table = [['Validity', 'Count']]
        table.append([ 'Valid', g_dash_stats['num_Valid'] ])
        table.append([ 'Invalid Length', g_dash_stats['num_InvalidLength'] ])
        table.append([ 'Invalid AS', g_dash_stats['num_InvalidAS'] ])
        stats['table_roa'] = table
        table_all = list(table)
        table_all.append([ 'Not Found', g_dash_stats['num_NotFound'] ])
        stats['table_all'] = table_all
        stats['latest_ts'] = g_dash_stats['latest_ts']
    except Exception, e:
        logging.exception ("stats with: " + e.message)
        print "stats: error " + e.message
    else:
        return render_template("stats.html", stats=stats)

## table handler
@app.route('/valid')
def valid():
    config = {'url' : '/valid_json', 'color' : 'success', 'state' : 'Valid'}
    return render_template("table.html", config=config)

@app.route('/invalid_as')
def invalid_as():
    config = {'url' : '/invalid_as_json', 'color' : 'danger', 'state' : 'InvalidAS'}
    return render_template("table.html", config=config)

@app.route('/invalid_len')
def invalid_len():
    config = {'url' : '/invalid_len_json', 'color' : 'warning', 'state' : 'InvalidLength'}
    return render_template("table.html", config=config)

## search handler
@app.route('/search', methods=['POST'])
def search():
    config = {'url' : '/search_json?search='+request.form['prefix'], 'color' : 'default', 'prefix' : request.form['prefix']}
    return render_template("search.html", config=config)

## table data as json
@app.route('/valid_json')
def valid_json():
    return _get_table_json('Valid')

@app.route('/invalid_as_json')
def invalid_as_json():
    return _get_table_json('InvalidAS')

@app.route('/invalid_len_json')
def invalid_len_table_json():
    return _get_table_json('InvalidLength')

@app.route('/search_json', methods=['GET'])
def search_json():
    search = request.args.get('search')
    validity_now = get_validation_prefix(config.DATABASE_CONN, search)
    ret = list()
    ret.append(validity_now[0])
    # validity_old = get_validation_history(config.DATABASE_CONN, validity_now[0]['prefix'])
    # cmp = ret[0]
    # for v in validity_old:
    #     if v['type'] != cmp['type']:
    #         ret.append(v)
    #         cmp = v
    #     elif (v['type'] == 'announcement') and ( (v['state'] != cmp['state']) or (v['origin'] != cmp['origin']) ):
    #         ret.append(v)
    #         cmp = v
    return json.dumps(ret, indent=2, separators=(',', ': '))
