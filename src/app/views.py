import json
from flask import render_template
from app import app
from dbconn import get_validation_stats, get_validation_tables

@app.route('/about')
def about():
    return render_template("about.html")
@app.route('/')

@app.route('/stats')
def stats():
    stats = get_validation_stats()
    chart_all = stats['table']
    chart_val = chart_all[:-1]
    data = {'chart_all': chart_all, 'chart_val': chart_val,
            'sum_all': stats['sum_all'], 'sum_val': stats['sum_val'],
            'latest_ts': stats['latest_ts']}
    return render_template("stats.html", stats=data)

@app.route('/tables')
def tables():
    tables = get_validation_tables()
    return render_template("tables.html", tables=tables)