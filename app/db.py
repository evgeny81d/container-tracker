import sqlite3
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

import click
from flask import current_app, g
from flask.cli import with_appcontext


def get_db():
    if 'db' not in g:
        g.db = MongoClient(current_app.config['DB_UPDATE'])
    return g.db


def close_db(e=None):
    db = g.pop('db', None)

    if db is not None:
        db.close()

# Register close_db() function with application
def register_close_db_func(app):
    app.teardown_appcontext(close_db)