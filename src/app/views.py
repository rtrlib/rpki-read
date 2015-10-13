import json
import logging
import sys
import socket

from netaddr import IPNetwork, IPAddress
from flask import render_template
from app import app

import config
if config.DATABASE_TYPE == 'mongodb':
    from mongodb import get_validation_stats, get_list
elif config.DATABASE_TYPE == 'postgresql':
    from postgresql import get_validation_stats, get_list
else:
    logging.critical("unknown database type!")
    sys.exit(1)

def _create_dropdown(data):
    hash_id = str(id(data))
    num_roas = len(data['matched']) + len(data['unmatched_as']) + len(data['unmatched_length'])
    html_dropdown = '<div class="dropdown">'
    html_dropdown += '<button class="btn btn-default dropdown-toggle" type="button" id="'+hash_id
    html_dropdown += '" data-toggle="dropdown" aria-haspopup="true" aria-expanded="false">'
    html_dropdown += 'Show ('+str(num_roas)+') <span class="caret"></span></button>'
    html_dropdown += '<ul class="dropdown-menu" style="padding:5px" aria-labelledby="'+hash_id+'">'
    html_dropdown += '<li class="dropdown-header">matched</li>'
    for m in data['matched']:
        html_dropdown += '<li>'
        html_dropdown += '<span>'+m['prefix']+' - '+m['max_length']+', '+m['asn']+'</span>'
        html_dropdown += '</li>'
    html_dropdown += '<li class="dropdown-header">unmatched AS</li>'
    for m in data['unmatched_as']:
        html_dropdown += '<li>'
        html_dropdown += '<span>'+m['prefix']+' - '+m['max_length']+', '+m['asn']+'</span>'
        html_dropdown += '</li>'
    html_dropdown += '<li class="dropdown-header">unmatched Length</li>'
    for m in data['unmatched_length']:
        html_dropdown += '<li>'
        html_dropdown += '<span>'+m['prefix']+' - '+m['max_length']+', '+m['asn']+'</span>'
        html_dropdown += '</li>'
    html_dropdown += '</ul></div>'

    return html_dropdown

def _create_html_table(data):
    html_table = '<table class="table table-striped">'
    html_table += '<thead><tr>'
    html_table += '<th>Prefix</th><th>Origin AS</th><th>Validity</th><th>ROAs (#)</th>'
    html_table += '</tr></thead>'
    html_table += '<tbody>'
    for d in data:
        html_table += '<tr>'
        html_table += '<td>'+d['prefix']+'</td>'
        html_table += '<td>'+d['origin']+'</td>'
        html_table += '<td>'+d['state']+'</td>'
        html_table += '<td>'+ _create_dropdown(d['roas']) + '</td>'
        html_table += '</tr>'
    html_table += '</tbody></table>'
    return html_table

def _get_table(state, color):
    dlist = get_list(config.DATABASE_CONN, state)
    nlist = sorted(dlist, key=lambda k: socket.inet_aton(str(IPNetwork(k['prefix']).ip)))
    table = _create_html_table(nlist)
    data = dict()
    data['table'] = table
    data['count'] = len(nlist)
    data['state'] = state
    data['color'] = color
    return render_template("vtable.html", data=data)

@app.route('/about')
def about():
    return render_template("about.html")

@app.route('/')
@app.route('/stats')
def stats():
    stats = get_validation_stats(config.DATABASE_CONN)
    table = [['Validity', 'Count']]
    table.append([ 'Valid', stats['num_valid'] ])
    table.append([ 'Invalid Length', stats['num_invalid_len'] ])
    table.append([ 'Invalid AS', stats['num_invalid_as'] ])
    stats['table_roa'] = table
    table_all = list(table)
    table_all.append([ 'Not Found', stats['num_not_found'] ])
    stats['table_all'] = table_all
    return render_template("stats.html", stats=stats)

@app.route('/valid')
def valid():
    return _get_table('Valid', 'success')

@app.route('/invalid_as')
def invalid_as():
    return _get_table('InvalidAS', 'danger')

@app.route('/invalid_len')
def invalid_len():
    return _get_table('InvalidLength', 'warning')
