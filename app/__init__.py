import os

from flask import Flask


def create_app(test_config=None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_mapping(
        SECRET_KEY='dev',
        DATABASE=os.path.join(app.instance_path, 'flaskr.sqlite'),
        DB_INIT = "mongodb://{}:{}@{}:27017/"\
            .format("OneInit", "<tkfzDtcnf844", "194.58.102.147"),
        DB_UPDATE = "mongodb://{}:{}@{}:27017/"\
            .format("OneUpdate", "<tkfzDtcnf844", "194.58.102.147"),
    )

    if test_config is None:
        # load the instance config, if it exists, when not testing
        app.config.from_pyfile('config.py', silent=True)
    else:
        # load the test config if passed in
        app.config.from_mapping(test_config)

    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    # Run registration for close_db() function
    from . import db
    db.register_close_db_func(app)

    # a simple page that says hello
    @app.route('/hello')
    def hello():
        return 'Hello, World!'

    return app