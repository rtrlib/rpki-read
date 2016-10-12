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
    from mongodb import get_validation_stats, get_validation_list, get_validation_prefix, get_validation_history
else:
    logging.critical("unknown database type!")
    sys.exit(1)

g_stats = dict()

def update_validation_stats():
    global g_stats
    g_stats = get_validation_stats(config.DATABASE_CONN)

@app.before_first_request
def initialize():
    apsched = BackgroundScheduler()
    update_validation_stats()
    apsched.add_job(update_validation_stats, 'interval', seconds=23)
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
    dash_stats = g_stats.copy()
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
    dash_stats = g_stats.copy()
    table = [['Validity', 'Count']]
    table.append([ 'Valid', dash_stats['num_Valid'] ])
    table.append([ 'Invalid Length', dash_stats['num_InvalidLength'] ])
    table.append([ 'Invalid AS', dash_stats['num_InvalidAS'] ])
    dash_stats['table_roa'] = table
    table_all = list(table)
    table_all.append([ 'Not Found', dash_stats['num_NotFound'] ])
    dash_stats['table_all'] = table_all
    dash_stats['source'] = config.BGPMON_SOURCE
    return render_template("stats.html", stats=dash_stats)

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
