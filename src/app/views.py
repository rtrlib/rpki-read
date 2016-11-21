"""
"""
import atexit
import codecs
import gc
import json
import logging
import markdown
import sys
import threading

from apscheduler.schedulers.background import BackgroundScheduler
from copy import deepcopy
from flask import render_template, Markup, request
from netaddr import IPNetwork, IPAddress
from app import app

import config
if config.DATABASE_TYPE == 'mongodb':
    from mongodb import *
else:
    logging.critical("unknown database type!")
    sys.exit(1)

g_stats = {'dash': {}, 'l24h': [], 'ipv4': {}, 'ipv6': {}}
g_stats_counter = config.UPDATE_INTERVAL_FACTOR
g_lock = threading.Lock()

#----- helper functions -----#
def _is_prefix(ipstr):
    """ check if ipstr is a valid IP prefix"""
    try:
        ipa = IPNetwork(ipstr).ip
        return len(str(ipa)) > 0
    except Exception as errmsg:
        logging.exception("IP address parse failed with: " + str(errmsg))
        return False

def _is_asn(asnstr):
    """ check if asnstr is a valid AS number"""
    if asnstr.upper().startswith('AS'):
        return True
    return False

def _get_table_json(state):
    """ return state table ass JSON """
    dlist = get_validation_list(config.DATABASE_CONN, state)
    data = dict()
    data['total'] = len(dlist)
    data['state'] = state
    data['rows'] = dlist
    return json.dumps(dlist, indent=2, separators=(',', ': '))

#----- update functions -----#
def update_dash_stats():
    """ update dashboard stats"""
    dash_stats = None
    try:
        dash_stats = get_dash_stats(config.DATABASE_CONN)
    except Exception as errmsg:
        logging.exception ("update_dash_stats, error: " + str(errmsg))
    else:
        if dash_stats != None:
            dash_stats['source'] = config.BGP_SOURCE
            dash_stats['rel_Valid'] = round((float(dash_stats['num_Valid'])/float(dash_stats['num_Total']))*100, 2)
            dash_stats['rel_InvalidLength'] = round((float(dash_stats['num_InvalidLength'])/float(dash_stats['num_Total']))*100, 2)
            dash_stats['rel_InvalidAS'] = round((float(dash_stats['num_InvalidAS'])/float(dash_stats['num_Total']))*100, 2)
            dash_stats['rel_NotFound'] = round((float(dash_stats['num_NotFound'])/float(dash_stats['num_Total']))*100, 2)
    return dash_stats

def update_last24h_stats():
    """ update stats over last 24h """
    last24h_stats = None
    try:
        last24h_stats = get_last24h_stats(config.DATABASE_CONN)
    except Exception as errmsg:
        logging.exception ("update_last24h_stats, error: " + str(errmsg))
    return last24h_stats

def update_ipversion_stats():
    """ update ip version specific stats """
    ipv4_stats = None
    ipv6_stats = None
    try:
        ipv4_stats, ipv6_stats = get_ipversion_stats(config.DATABASE_CONN)
    except Exception as errmsg:
        logging.exception("update_ipversion_stats, error: " + str(errmsg))
    return ipv4_stats, ipv6_stats

def update_stats():
    """ update all stats """
    global g_stats, g_stats_counter, g_lock
    g_stats_counter += 1
    dash_stats = update_dash_stats()
    if dash_stats != None:
        with g_lock:
            g_stats['dash'] = dash_stats
    # end if
    if g_stats_counter > config.UPDATE_INTERVAL_FACTOR:
        g_stats_counter = 0
        l24h_stats = update_last24h_stats()
        ipv4_stats, ipv6_stats = update_ipversion_stats()
        if l24h_stats != None:
            with g_lock:
                g_stats['l24h'] = l24h_stats
        if ipv4_stats != None:
            with g_lock:
                g_stats['ipv4'] = ipv4_stats
        if ipv6_stats != None:
            with g_lock:
                g_stats['ipv6'] = ipv6_stats
    # end if g_stats_counter
    gc.collect()

@app.before_first_request
def initialize():
    # init logger
    logger = logging.getLogger("rpki-read")
    logger.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    # init update job
    scheduler = BackgroundScheduler()
    scheduler.start()
    update_stats()
    scheduler.add_job(func=update_stats,
                      trigger='interval',
                      seconds=config.UPDATE_INTERVAL_STATS,
                      id="job_stats",
                      name="Update stats every UPDATE_INTERVAL_STATS seconds",
                      replace_existing=True)
    atexit.register(lambda: scheduler.shutdown())

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
    global g_lock
    lstats = {}
    with g_lock:
        if g_stats['dash'] != None:
            lstats = deepcopy(g_stats['dash'])
    # end lock
    return render_template("dashboard.html", stats=lstats)

## stats handler
@app.route('/stats')
def stats():
    global g_lock
    lstats = dict()
    with g_lock:
        # ipv4 origin stats
        if ('num_NotFound' in g_stats['ipv4']) and (g_stats['ipv4']['num_NotFound'] > 0):
            lstats['ipv4'] = '1'
            lstats['ipv4_data'] = deepcopy(g_stats['ipv4'])
        if ('num_NotFound' in g_stats['ipv6']) and (g_stats['ipv6']['num_NotFound'] > 0):
            lstats['ipv6'] = '1'
            lstats['ipv6_data'] = deepcopy(g_stats['ipv6'])
        lstats['latest_ts'] = deepcopy(g_stats['dash']['latest_ts'])
        lstats['source'] = deepcopy(g_stats['dash']['source'])
        lstats['last24h'] = deepcopy(g_stats['l24h'])
    # end lock
    return render_template("stats.html", stats=lstats)

## table handler
@app.route('/valid')
def valid():
    config_json = {'url' : '/valid_json', 'color' : 'success', 'state' : 'Valid'}
    return render_template("table.html", config=config_json)

@app.route('/invalid_as')
def invalid_as():
    config_json = {'url' : '/invalid_as_json', 'color' : 'danger', 'state' : 'InvalidAS'}
    return render_template("table.html", config=config_json)

@app.route('/invalid_len')
def invalid_len():
    config_json = {'url' : '/invalid_len_json', 'color' : 'warning', 'state' : 'InvalidLength'}
    return render_template("table.html", config=config_json)

## search handler
@app.route('/search', methods=['POST'])
def search():
    config_json = {'url' : '/search_json?search='+request.form['prefix'],
                   'color' : 'default', 'prefix' : request.form['prefix']}
    return render_template("search.html", config=config_json)

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
    if _is_prefix(search):
        validity_now = get_validation_prefix(config.DATABASE_CONN, search)
    elif _is_asn(search):
        validity_now = get_validation_origin(config.DATABASE_CONN, search)
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
