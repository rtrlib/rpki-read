import codecs
import json
import logging
import markdown
import sys
import socket

from apscheduler.schedulers.background import BackgroundScheduler
from netaddr import IPNetwork, IPAddress
from flask import render_template, Markup
from app import app

import config
if config.DATABASE_TYPE == 'mongodb':
    from mongodb import get_validation_stats, get_list
else:
    logging.critical("unknown database type!")
    sys.exit(1)

g_stats = dict()

def update_validation_stats():
    global g_stats
    g_stats = get_validation_stats(config.DATABASE_CONN)

@app.before_first_request
def initialize():
    update_validation_stats()
    apsched = BackgroundScheduler()
    update_validation_stats()
    apsched.add_job(update_validation_stats, 'interval', seconds=23)
    apsched.start()

def _get_table_json(state):
    dlist = get_list(config.DATABASE_CONN, state)
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

## stats handler
@app.route('/')
@app.route('/stats')
def stats():
    #stats = get_validation_stats(config.DATABASE_CONN)
    l_stats = g_stats.copy()
    table = [['Validity', 'Count']]
    table.append([ 'Valid', l_stats['num_Valid'] ])
    table.append([ 'Invalid Length', l_stats['num_InvalidLength'] ])
    table.append([ 'Invalid AS', l_stats['num_InvalidAS'] ])
    l_stats['table_roa'] = table
    table_all = list(table)
    table_all.append([ 'Not Found', l_stats['num_NotFound'] ])
    l_stats['table_all'] = table_all
    return render_template("stats.html", stats=l_stats)

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
