from flask import (
    Blueprint, flash, g, redirect, render_template, request, url_for
)
from werkzeug.exceptions import abort

#from flaskr.auth import login_required
from ships_tracker.db import get_conn

bp = Blueprint('home', __name__)

@bp.route('/')
def index():
    conn = get_conn()
    content = {}
    content["ships"] = conn.one.ships.count_documents({})
    return render_template('home/index.html', content=content)
