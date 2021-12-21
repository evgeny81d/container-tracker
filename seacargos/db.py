import sqlite3
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

import click
from flask import current_app, g
from flask.cli import with_appcontext


def get_conn():
    if 'conn' not in g:
        g.conn = MongoClient(current_app.config['DB_UPDATE'])
    return g.conn

def close_conn(exception):
    conn = g.pop('conn', None)

    if conn is not None:
        conn.close()

# Register close_conn() function with application
def init_app(app):
    app.teardown_appcontext(close_conn)