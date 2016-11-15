import codecs
import gc
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

g_dash_stats = dict()
g_ipv4_stats = dict()
g_ipv6_stats = dict()
g_last24h_stats = list()
g_stats_counter = config.UPDATE_INTERVAL_FACTOR

def update_dash_stats():
    global g_dash_stats
    try:
        dash_stats = get_dash_stats(config.DATABASE_CONN)
        if dash_stats != None:
            dash_stats['source'] = config.BGP_SOURCE
            dash_stats['rel_Valid'] = round( (float(dash_stats['num_Valid'])/float(dash_stats['num_Total']))*100 , 2)
            dash_stats['rel_InvalidLength'] = round( (float(dash_stats['num_InvalidLength'])/float(dash_stats['num_Total']))*100 , 2)
            dash_stats['rel_InvalidAS'] = round( (float(dash_stats['num_InvalidAS'])/float(dash_stats['num_Total']))*100 , 2)
            dash_stats['rel_NotFound'] = round( (float(dash_stats['num_NotFound'])/float(dash_stats['num_Total']))*100 , 2)
            g_dash_stats = dash_stats
    except Exception, e:
        logging.exception ("update_dash_stats, error: " + e.message)
        print "update_dash_stats, error: " + e.message

def update_last24h_stats():
    global g_last24h_stats
    try:
        last24h_stats = get_last24h_stats(config.DATABASE_CONN)
        if last24h_stats != None:
            g_last24h_stats = last24h_stats
    except Exception, e:
        logging.exception ("update_last24h_stats, error: " + e.message)
        print "update_last24h_stats, error: " + e.message
    gc.collect()

def update_ipversion_stats():
    global g_ipv4_stats, g_ipv6_stats
    try:
        ipv4_stats, ipv6_stats = get_ipversion_stats(config.DATABASE_CONN)
        if ipv4_stats != None:
            g_ipv4_stats = ipv4_stats
        if ipv6_stats != None:
            g_ipv6_stats = ipv6_stats
    except Exception, e:
        logging.exception ("update_ipversion_stats, error: " + e.message)
        print "update_ipversion_stats, error: " + e.message

@app.before_first_request
def initialize():
    apsched = BackgroundScheduler()
    update_dash_stats()
    update_last24h_stats()
    update_ipversion_stats()
    apsched.add_job(update_dash_stats, 'interval', seconds=config.UPDATE_INTERVAL_STATS, id="job_dash")
    apsched.add_job(update_last24h_stats, 'interval', seconds=config.UPDATE_INTERVAL_STATS*config.UPDATE_INTERVAL_FACTOR, id="job_last24h")
    apsched.add_job(update_ipversion_stats, 'interval', seconds=config.UPDATE_INTERVAL_STATS*config.UPDATE_INTERVAL_FACTOR, id="job_ipversion")
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
    if g_dash_stats != None:
        return render_template("dashboard.html", stats=g_dash_stats)

## stats handler
@app.route('/stats')
def stats():
    stats=dict()
    try:
        # ipv4 origin stats
        if ('num_NotFound' in g_ipv4_stats) and (g_ipv4_stats['num_NotFound'] > 0):
        #if ('num_NotFound' in g_ipv4_stats):
            stats['ipv4'] = '1'
            stats['ipv4_data'] = g_ipv4_stats
        if ('num_NotFound' in g_ipv6_stats) and (g_ipv6_stats['num_NotFound'] > 0):
        #if ('num_NotFound' in g_ipv6_stats):
            stats['ipv6'] = '1'
            stats['ipv6_data'] = g_ipv6_stats
        stats['latest_ts'] = g_dash_stats['latest_ts']
        stats['source'] = g_dash_stats['source']
        stats['last24h'] = g_last24h_stats
        return render_template("stats.html", stats=stats)
    except Exception, e:
        logging.exception ("stats with: " + e.message)
        print "stats: error " + e.message

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
    if validity_now != None:
        ret.extend(validity_now)
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
