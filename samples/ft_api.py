#!/usr/bin/env python

import sys
from sys import stdout
sys.path.append('/usr/local/lib/python2.7/site-packages/')
import os
import json, re, time, traceback, datetime, os, logging, logging.config
from json import dumps
from inspect import getsourcefile
from os.path import abspath
from distutils.util import strtobool
from flask import abort, url_for, request, make_response, jsonify
## doc: http://flask.pocoo.org/docs
from flask import Flask, g, Response, request
## doc: https://neo4j.com/docs/api/python-driver/current/
import neo4j.v1
from neo4j.v1 import GraphDatabase, basic_auth
from neo4j.util import watch
import logging

## verify environment type ("production", "stage", "performance" "development", "qa", etc)
global PYTHON_ENV, CONFIG, EXE_PATH
## default = 'development'
PYTHON_ENV = os.getenv('PYTHON_ENV', 'development')
## load config file
exe_file_abspath = abspath(getsourcefile(lambda:0))
EXE_PATH = exe_file_abspath.split('/')
EXE_PATH = exe_file_abspath.replace(EXE_PATH[len(EXE_PATH) - 1], "")
CONFIG = json.load(open(EXE_PATH + 'conf/' + PYTHON_ENV + '.json'))

global COMPILED_INPUT_RE, COMPILED_RE_ESCP
## pre-compile RE for use with escaping special chars in cypher
COMPILED_RE_ESCP = re.compile(r'([\.\\\+\*\?\[\^\]\$\(\)\{\}\!\<\>\|\:\-])', re.IGNORECASE)
COMPILED_INPUT_RE = re.compile('[^a-zA-Z0-9\_\s\-\'\"\&]+$', re.IGNORECASE)

## initialize flask
app = Flask(__name__)

class LoggerConfig:
    dict_config = {
        'version': 1,
        'disable_existing_loggers': CONFIG['logging']['dis_existing_loggers']['default'],
        'formatters': {
            'standard': { 'format': '%(asctime)s - %(levelname)s - %(message)s'},
                          # '%(message)s - [in %(pathname)s:%(lineno)d]'},
            'short': { 'format': '%(message)s' }
        },
        'handlers': {
            'default': {
                'level': 'DEBUG',
                'formatter': 'standard',
                'class': 'logging.handlers.RotatingFileHandler',
                'filename': EXE_PATH + CONFIG['logging']['main_file']['default'],
                'maxBytes': CONFIG['logging']['max_bytes']['default'],
                'backupCount': CONFIG['logging']['backup_file_num']['default']
            },
            'debug': {
                'level': 'DEBUG',
                'formatter': 'standard',
                'class': 'logging.StreamHandler'
            },
           'console': {
            'class': 'logging.StreamHandler',
            'level': 'DEBUG'
           },
        },
        'loggers': {
            CONFIG['logging']['logger_name']['default']: {
                'handlers': ['default'],
                'level': 'DEBUG',
                'propagate': True },
            'werkzeug': { 'propagate': True },
        }
        # 'root': { 'level': 'DEBUG', 'handlers': ['console'] }
    }

app.config['LOGGER_HANDLER_POLICY'] = 'always'  # 'always' (default), 'never',  'production', 'debug'
app.config['LOGGER_NAME'] = CONFIG['logging']['logger_name']['default'] # define which logger to use for Flask
app.logger #  initialize logger

if len(sys.argv) > 1 and sys.argv[1] == 'debug':
    app.debug = True

## Load the logging config
logging.config.dictConfig(LoggerConfig.dict_config)

if CONFIG['neo4j']['bolt_endpoint']['debug_enabled']['default']:
    ## turn on debug mode for neo4j bolt activities
    watch('neo4j.bolt', logging.DEBUG, stdout)
    app.logger.info('Debug logging for bolt is activated.')

## Initialize neo4j driver and connect to neo4 database
try:
    #password = os.getenv("NEO4J_PASSWORD")
    password = CONFIG['neo4j']['bolt_endpoint']['credentials']['password']
    username = CONFIG['neo4j']['bolt_endpoint']['credentials']['username']
    bolt_endpoint = CONFIG['neo4j']['bolt_endpoint']['default']
    driver = GraphDatabase.driver(bolt_endpoint, auth=basic_auth(username, password))
    app.logger.info('Successfullly initialized neo4j driver')
except:
    app.logger.critical('Error initializing neo4j driver')
    traceback.print_exc()
    sys.exit(1)

################################################################################

## Add additional headers to the response, after the request is processed.
@app.after_request
def after_request(response):
    response.headers['Content-Type'] = 'application/json'
    response.headers['Server']  = CONFIG['server']['user_agent']['default']
    return response

## HTTP 404 error response.
@app.errorhandler(404)
def not_found(error):
    app.logger.error(request.environ.get('REMOTE_ADDR') + ' - ' + request.method + ' - ' + request.path + ' - ' + request.query_string + ' - - ' + CONFIG['messaging']['error']['210']['msg'])
    return make_response(jsonify(
        set_err_response(
            CONFIG['messaging']['error']['210']['msg'],
            CONFIG['messaging']['error']['210']['code']
        )
    ), CONFIG['messaging']['error']['210']['http_code'])

## HTTP 405 error response.
@app.errorhandler(405)
def invalid_method(error):
    app.logger.warning(request.environ.get('REMOTE_ADDR') + ' - ' + request.method + ' - ' + request.path + ' - ' + request.query_string + ' - - ' + CONFIG['messaging']['warning']['201']['msg'])
    return make_response(jsonify(
        set_err_response(
            CONFIG['messaging']['warning']['201']['msg'],
            CONFIG['messaging']['warning']['201']['code']
        )
    ), CONFIG['messaging']['warning']['201']['http_code'])

## HTTP 500 error response.
@app.errorhandler(500)
def server_error(error):
    app.logger.critical(request.environ.get('REMOTE_ADDR') + ' - ' + request.method + ' - ' + request.path + ' - ' + request.query_string + ' - - ' + CONFIG['messaging']['error']['209']['msg'])
    print(error)
    return make_response(jsonify(
        set_err_response(
            CONFIG['messaging']['error']['209']['msg'],
            CONFIG['messaging']['error']['209']['code']
        )
    ), CONFIG['messaging']['error']['209']['http_code'])

## Retrieve a db session
def get_db_session():
    if not hasattr(g, 'neo4j_db'):
        g.neo4j_db = driver.session()
    return g.neo4j_db

## Close the db connection when app shuts down.
@app.teardown_appcontext
def close_db_session(error):
    if hasattr(g, 'neo4j_db'):
        g.neo4j_db.close()

## Root API
@app.route('/')
def get_index():
    return make_response(jsonify({
        'api_name': CONFIG['server']['user_agent']['default'],
        'api_doc_url': CONFIG['server']['api_doc_url']['default'],
        'apis': [
            {
                'api_url_ep': '/api/region/<code or id>/{api_url_params}?{params}',
                'api_url_params': [
                    {
                        'code': None,
                        'supported_values': 'grtphl,grtnj',
                        'example': '/api/region/code/grtphl'
                    }
                ],
                'params': [
                    {
                        'name': 'cat_types',
                        'supported_values': 'ftr,veg,org'
                    },
                    {
                        'name': 'subcat_names',
                        'supported_values': 'apparel,banana,chocolate,coffee,dried fruit,ice cream,jewelry,nuts,sugar,tea'
                    },
                    {
                        'name': 'maincat_names',
                        'supported_values': 'accessories,beverages,desserts,gifts,grocery,health & beauty,home goods'
                    },
                    {
                        'name': 'org_names',
                        'supported_values': 'ftphl'
                    },
                    {
                        'name': 'ft_min_rating',
                        'supported_values': '1,2,3,4,5'
                    }
                ]
            }
        ]
    }), 200)

# @app.route("/graph")
# def get_graph():
#     db = get_db_session()
#     results = db.run("MATCH (m:Movie)<-[:ACTED_IN]-(a:Person) "
#              "RETURN m.title as movie, collect(a.name) as cast "
#              "LIMIT {limit}", {"limit": request.args.get("limit", 100)})
#     nodes = []
#     rels = []
#     i = 0
#     for record in results:
#         nodes.append({"title": record["movie"], "label": "movie"})
#         target = i
#         i += 1
#         for name in record['cast']:
#             actor = {"title": name, "label": "actor"}
#             try:
#                 source = nodes.index(actor)
#             except ValueError:
#                 nodes.append(actor)
#                 source = i
#                 i += 1
#             rels.append({"source": source, "target": target})
#     return Response(dumps({"nodes": nodes, "links": rels}),
#                     mimetype="application/json")

## Search API
## TO-DO:
## [ ] 1. Pagination
## [ ] 2. Better logging
@app.route('/search', methods=['GET'])
def search():
    q = ''
    r = ''
    try:
        q = request.args.get('q')
        req_meta_info = request.environ.get('REMOTE_ADDR') + ' - ' + request.method + ' - ' + request.path + ' - ' + request.query_string + ' - %s' % q + ' - '
        ## verify input
        if q is not None and q != '':
            q_len = len(q)
            if COMPILED_INPUT_RE.match(q) is not None or not (q_len >= 1 and q_len <= 64):
                r = make_response(jsonify(
                    set_err_response(
                        CONFIG['messaging']['error']['202']['msg'],
                        CONFIG['messaging']['error']['202']['code']
                    )
                ), CONFIG['messaging']['error']['202']['http_code'])

                return r
        else:
            r = make_response(jsonify(
                set_err_response(
                    CONFIG['messaging']['error']['202']['msg'],
                    CONFIG['messaging']['error']['202']['code']
                )
            ), CONFIG['messaging']['error']['202']['http_code'])

            return r

    except KeyError:
        if CONFIG['neo4j']['debug_enabled']['default'] == True:
            traceback.print_exc()

        r = make_response(jsonify(
            set_err_response(
                CONFIG['messaging']['error']['202']['msg'],
                CONFIG['messaging']['error']['202']['code']
            )
        ), CONFIG['messaging']['error']['202']['http_code'])
    else:
        try:
            q_filters = []
            db_query_opts = {}
            if request.args.get('q_filters') is not None:
                if re.search(',', request.args.get('q_filters')):
                    q_filters = req_info['q_filters'].split(',')
                else:
                    q_filters.append(request.args.get('q_filters'))
            else:
                q_filters.append('query')

            db_query_opts.update({ 'q_filters': q_filters })

            if request.args.get('country_codes') is not None:
                db_query_opts.update({ 'country_codes': request.args.get('country_codes') })

            if request.args.get('country_ids') is not None:
                db_query_opts.update({ 'country_ids': request.args.get('country_ids') })

            if request.args.get('province_codes') is not None:
                db_query_opts.update({ 'province_codes': request.args.get('province_codes') })

            if request.args.get('province_ids') is not None:
                db_query_opts.update({ 'province_ids': request.args.get('province_ids') })

            if request.args.get('cat_types') is not None:
                db_query_opts.update({ 'cat_types': request.args.get('cat_types') })
            else:
                db_query_opts.update({ 'cat_types': 'ftr' })

            if request.args.get('biz_status') is not None:
                db_query_opts.update({ 'biz_status': request.args.get('biz_status') })
            else:
                db_query_opts.update({ 'biz_status': 'A' })

            if request.args.get('rg_codes') is not None:
                db_query_opts.update({ 'rg_codes': request.args.get('rg_codes') })

            if request.args.get('org_codes') is not None:
                db_query_opts.update({ 'org_codes': request.args.get('org_codes') })

            if request.args.get('ft_min_rating') is not None:
                db_query_opts.update({ 'ft_min_rating': request.args.get('ft_min_rating') })

            if q is not None or q != '':
                db_query_opts.update({ 'q': q })

            query = build_db_query_business(db_query_opts)
            r = exe_query(query)
            r.update({ 'result_type': 'biz' })
            r = respond_records(r)

        except:
            if CONFIG['server']['debug_enabled']['default'] == True:
                traceback.print_exc()

            r = make_response(jsonify(
                set_err_response(
                    CONFIG['messaging']['error']['209']['msg'],
                    CONFIG['messaging']['error']['209']['code']
                )
            ), CONFIG['messaging']['error']['209']['http_code'])

    return r

## Example: /country/us,cn/province/pa,nj?city_name=coronado
## TO-DO:
## [ ] 1. Pagination
## [ ] 2. Better logging
@app.route('/country/<c_type>/<country_ids>/province/<p_type>/<province_ids>/business', methods=['GET'])
def get_business(c_type = None, country_ids = None, p_type = None, province_ids = None):
    req_meta_info = request.environ.get('REMOTE_ADDR') + ' - ' + request.method + ' - ' + request.path + ' - ' + request.query_string

    try:
        db_query_opts = {}
        r = ''

        if c_type == 'code':
            db_query_opts.update({ 'country_codes': country_ids })
        else:
            db_query_opts.update({ 'country_ids': country_ids })

        if p_type == 'code':
            db_query_opts.update({ 'province_codes': province_ids })
        else:
            db_query_opts.update({ 'province_ids': province_ids })

        if request.args.get('city_names') is not None:
            db_query_opts.update({ 'city_names': request.args.get('city_names') })

        if request.args.get('city_ids') is not None:
            db_query_opts.update({ 'city_ids': request.args.get('city_ids') })

        if request.args.get('rg_codes') is not None:
            db_query_opts.update({ 'rg_codes': request.args.get('rg_codes') })

        if request.args.get('rg_ids') is not None:
            db_query_opts.update({ 'rg_ids': request.args.get('rg_ids') })

        if request.args.get('neigh_names') is not None:
            db_query_opts.update({ 'neigh_names': request.args.get('neigh_names') })

        if request.args.get('neigh_ids') is not None:
            db_query_opts.update({ 'neigh_ids': request.args.get('neigh_ids') })

        if request.args.get('cat_types') is not None:
            db_query_opts.update({ 'cat_types': request.args.get('cat_types') })
        else:
            db_query_opts.update({ 'cat_types': 'ftr' })

        if request.args.get('biz_status') is not None:
            db_query_opts.update({ 'biz_status': request.args.get('biz_status') })
        else:
            db_query_opts.update({ 'biz_status': 'A' })

        if request.args.get('org_codes') is not None:
            db_query_opts.update({ 'org_codes': request.args.get('org_codes') })

        if request.args.get('zipcodes') is not None:
            db_query_opts.update({ 'zipcodes': request.args.get('zipcodes') })

        if request.args.get('ft_min_rating') is not None:
            db_query_opts.update({ 'ft_min_rating': request.args.get('ft_min_rating') })

        if request.args.get('subcat_names') is not None:
            db_query_opts.update({ 'subcat_names': request.args.get('subcat_names') })

        if request.args.get('maincat_names') is not None:
            db_query_opts.update({ 'maincat_names': request.args.get('maincat_names') })

        query = build_db_query_business(db_query_opts)
        r = exe_query(query)
        r.update({ 'result_type': 'biz' })
        r = respond_records(r)
    except:
        if CONFIG['server']['debug_enabled']['default'] == True:
            traceback.print_exc()

        r = make_response(jsonify(
            set_err_response(
                CONFIG['messaging']['error']['209']['msg'],
                CONFIG['messaging']['error']['209']['code']
            )
        ), CONFIG['messaging']['error']['209']['http_code'])

    return r

@app.route('/country', methods=['GET'])
def get_country():
    req_meta_info = request.environ.get('REMOTE_ADDR') + ' - ' + request.method + ' - ' + request.path + ' - ' + request.query_string

    try:
        db_query_opts = {
            'country': {}
        }
        r = {}
        query = build_db_query_loc(db_query_opts)
        r = exe_query(query)
        r.update({ 'result_type': 'loc_cn' })
        r = respond_records(r)
    except:
        if CONFIG['server']['debug_enabled']['default'] == True:
            traceback.print_exc()

        r = make_response(jsonify(
            set_err_response(
                CONFIG['messaging']['error']['209']['msg'],
                CONFIG['messaging']['error']['209']['code']
            )
        ), CONFIG['messaging']['error']['209']['http_code'])

    return r

@app.route('/country/<cn_type>/<country_ids>/province', methods=['GET'])
def get_province(cn_type = None, country_ids = None):
    req_meta_info = request.environ.get('REMOTE_ADDR') + ' - ' + request.method + ' - ' + request.path + ' - ' + request.query_string
    cn_i_type = 'country_ids'
    if cn_type == 'code':
        cn_i_type = 'country_codes'

    try:
        db_query_opts = {
            'country': {
                'params':  [
                    {
                        'input_type': cn_i_type,
                        'value': country_ids
                    }
                ]
            },
            'province': {}
        }
        r = {}
        query = build_db_query_loc(db_query_opts)
        r = exe_query(query)
        r.update({ 'result_type': 'loc_prov' })
        r = respond_records(r)
    except:
        if CONFIG['server']['debug_enabled']['default'] == True:
            traceback.print_exc()
        r = make_response(jsonify(
            set_err_response(
                CONFIG['messaging']['error']['209']['msg'],
                CONFIG['messaging']['error']['209']['code']
            )
        ), CONFIG['messaging']['error']['209']['http_code'])

    return r

@app.route('/country/<cn_type>/<country_ids>/province/<p_type>/<province_ids>/city', methods=['GET'])
def get_city(cn_type = None, country_ids = None, p_type = None, province_ids = None):
    req_meta_info = request.environ.get('REMOTE_ADDR') + ' - ' + request.method + ' - ' + request.path + ' - ' + request.query_string

    cn_i_type = 'country_ids'
    if cn_type == 'code':
        cn_i_type = 'country_codes'

    p_i_type = 'province_ids'
    if p_type == 'code':
        p_i_type = 'province_codes'

    try:
        db_query_opts = {
            'country': {
                'params':  [
                    {
                        'input_type': cn_i_type,
                        'value': country_ids
                    }
                ]
            },
            'province': {
                'params':  [
                    {
                        'input_type': p_i_type,
                        'value': province_ids
                    }
                ]
            },
            'city': {}
        }

        r = {}
        query = build_db_query_loc(db_query_opts)
        r = exe_query(query)
        r.update({ 'result_type': 'loc_city' })
        r = respond_records(r)

    except:
        if CONFIG['server']['debug_enabled']['default'] == True:
            traceback.print_exc()

        r = make_response(jsonify(
            set_err_response(
                CONFIG['messaging']['error']['209']['msg'],
                CONFIG['messaging']['error']['209']['code']
            )
        ), CONFIG['messaging']['error']['209']['http_code'])

    return r

## /region/id|code></<reg_codes>
## TO-DO:
## [ ] 1. Pagination
## [ ] 2. Better logging
@app.route('/region/<input_type>/<rg_codes>', methods=['GET'])
def get_by_regions(input_type = None, rg_codes = None):

    req_meta_info = request.environ.get('REMOTE_ADDR') + ' - ' + request.method + ' - ' + request.path + ' - ' + request.query_string + ' - %s' % rg_codes + ' - '

    try:
        db_query_opts = {}
        r = ''

        if input_type == 'code' or input_type == 'id':
            if input_type == 'code':
                db_query_opts.update({ 'rg_codes': rg_codes })
            else:
                db_query_opts.update({ 'rg_ids': rg_codes })

            if request.args.get('cat_types') is not None:
                db_query_opts.update({ 'cat_types': request.args.get('cat_types') })
            else:
                db_query_opts.update({ 'cat_types': 'ftr' })

            if request.args.get('biz_status') is not None:
                db_query_opts.update({ 'biz_status': request.args.get('biz_status') })
            else:
                db_query_opts.update({ 'biz_status': 'A' })

            if request.args.get('org_codes') is not None:
                db_query_opts.update({ 'org_codes': request.args.get('org_codes') })

            if request.args.get('zipcodes') is not None:
                db_query_opts.update({ 'zipcodes': request.args.get('zipcodes') })

            if request.args.get('ft_min_rating') is not None:
                db_query_opts.update({ 'ft_min_rating': request.args.get('ft_min_rating') })

            if request.args.get('subcat_names') is not None:
                db_query_opts.update({ 'subcat_names': request.args.get('subcat_names') })

            if request.args.get('maincat_names') is not None:
                db_query_opts.update({ 'maincat_names': request.args.get('maincat_names') })

            query = build_db_query_business(db_query_opts)
            r = exe_query(query)

        else:
            r = make_response(jsonify(
                set_err_response(
                    CONFIG['messaging']['error']['210']['msg'],
                    CONFIG['messaging']['error']['210']['code']
                )
            ), CONFIG['messaging']['error']['210']['http_code'])
    except:
        if CONFIG['server']['debug_enabled']['default'] == True:
            traceback.print_exc()

        r = make_response(jsonify(
            set_err_response(
                CONFIG['messaging']['error']['209']['msg'],
                CONFIG['messaging']['error']['209']['code']
            )
        ), CONFIG['messaging']['error']['209']['http_code'])

    return r

@app.route('/admin/country/<cn_type>/<country_ids>/province/<p_type>/<province_ids>/city/<c_type>/<c_ids>/business', methods=['GET'])
def get_business_by_city(cn_type = None, country_ids = None, p_type = None, province_ids = None, c_type = None, c_ids = None):
    req_meta_info = request.environ.get('REMOTE_ADDR') + ' - ' + request.method + ' - ' + request.path + ' - ' + request.query_string
    cn_i_type = 'country_ids'
    if cn_type == 'code':
        cn_i_type = 'country_codes'

    p_i_type = 'province_ids'
    if p_type == 'code':
        p_i_type = 'province_codes'

    c_i_type = 'city_ids'
    if c_type == 'name':
        c_i_type = 'city_names'

    try:
        db_query_opts = {
            'country': {
                'params':  [
                    {
                        'input_type': cn_i_type,
                        'value': country_ids
                    }
                ]
            },
            'province': {
                'params':  [
                    {
                        'input_type': p_i_type,
                        'value': province_ids
                    }
                ]
            },
            'city': {
                'params':  [
                    {
                        'input_type': c_i_type,
                        'value': c_ids
                    }
                ]
            }
        }
        r = {}
        query = build_db_query_by_country(db_query_opts)
        r = exe_query(query)
        r.update({ 'result_type': 'biz_country' })
        r = respond_records(r)
    except:
        if CONFIG['server']['debug_enabled']['default'] == True:
            traceback.print_exc()
        r = make_response(jsonify(
            set_err_response(
                CONFIG['messaging']['error']['209']['msg'],
                CONFIG['messaging']['error']['209']['code']
            )
        ), CONFIG['messaging']['error']['209']['http_code'])

    return r

@app.route('/admin/country/<cn_type>/<country_ids>/province/<p_type>/<province_ids>/business', methods=['GET'])
def get_business_by_province(cn_type = None, country_ids = None, p_type = None, province_ids = None):
    req_meta_info = request.environ.get('REMOTE_ADDR') + ' - ' + request.method + ' - ' + request.path + ' - ' + request.query_string
    cn_i_type = 'country_ids'
    if cn_type == 'code':
        cn_i_type = 'country_codes'

    p_i_type = 'province_ids'
    if p_type == 'code':
        p_i_type = 'province_codes'

    try:
        db_query_opts = {
            'country': {
                'params':  [
                    {
                        'input_type': cn_i_type,
                        'value': country_ids
                    }
                ]
            },
            'province': {
                'params':  [
                    {
                        'input_type': p_i_type,
                        'value': province_ids
                    }
                ]
            },
            'city': {}
        }
        r = {}
        query = build_db_query_by_country(db_query_opts)
        r = exe_query(query)
        r.update({ 'result_type': 'biz_country' })
        r = respond_records(r)
    except:
        if CONFIG['server']['debug_enabled']['default'] == True:
            traceback.print_exc()
        r = make_response(jsonify(
            set_err_response(
                CONFIG['messaging']['error']['209']['msg'],
                CONFIG['messaging']['error']['209']['code']
            )
        ), CONFIG['messaging']['error']['209']['http_code'])

    return r

@app.route('/admin/country/<cn_type>/<country_ids>/business', methods=['GET'])
def get_business_by_country(cn_type = None, country_ids = None):
    req_meta_info = request.environ.get('REMOTE_ADDR') + ' - ' + request.method + ' - ' + request.path + ' - ' + request.query_string
    cn_i_type = 'country_ids'
    if cn_type == 'code':
        cn_i_type = 'country_codes'

    try:
        db_query_opts = {
            'country': {
                'params':  [
                    {
                        'input_type': cn_i_type,
                        'value': country_ids
                    }
                ]
            },
            'province': {}
        }
        r = {}
        query = build_db_query_by_country(db_query_opts)
        r = exe_query(query)
        r.update({ 'result_type': 'biz_country' })
        r = respond_records(r)
    except:
        if CONFIG['server']['debug_enabled']['default'] == True:
            traceback.print_exc()
        r = make_response(jsonify(
            set_err_response(
                CONFIG['messaging']['error']['209']['msg'],
                CONFIG['messaging']['error']['209']['code']
            )
        ), CONFIG['messaging']['error']['209']['http_code'])

    return r

# @app.route("/movie/<title>")
# def get_movie(title):
#     db = get_db_session()
#     results = db.run("MATCH (movie:Movie {title:{title}}) "
#              "OPTIONAL MATCH (movie)<-[r]-(person:Person) "
#              "RETURN movie.title as title,"
#              "collect([person.name, "
#              "         head(split(lower(type(r)), '_')), r.roles]) as cast "
#              "LIMIT 1", {"title": title})
#
#     result = results.single();
#     return Response(dumps({"title": result['title'],
#                            "cast": [serialize_cast(member)
#                                     for member in result['cast']]}),
#                     mimetype="application/json")

def get_location(loc):
    try:
        db_query_opts = {}
        r = {}
        for p in loc['params']:
            db_query_opts.update({ p['param_name']: True })
            if 'input_type' in p:
                db_query_opts.update({ p['input_type']: p['ids'] })

        query = build_db_query_loc(db_query_opts)
        r = exe_query(query)
        if 'loc_type' in loc:
            r.update({ 'result_type': loc['loc_type'] })
        r = respond_records(r)
    except:
        if CONFIG['server']['debug_enabled']['default'] == True:
            traceback.print_exc()

        r = make_response(jsonify(
            set_err_response(
                CONFIG['messaging']['error']['209']['msg'],
                CONFIG['messaging']['error']['209']['code']
            )
        ), CONFIG['messaging']['error']['209']['http_code'])

    return r

def build_db_query_by_country(req_info):

    country_all_q = []
    prov_all_q = []
    city_all_q = []
    cat_types_all_q = []
    country_q = province_q = city_q = ''

    if 'cat_types' in req_info:
        if req_info['cat_types'] is not None:
            cat_types_all_q.append(set_re_subquery('s.type_code', req_info['cat_types'], 'string'))
    if cat_types_all_q:
        type_code_q +=  type_code_match_q + ' WHERE (' + build_subquery(cat_types_all_q, 'OR') + ')'

    if 'country' in req_info:
        country_q = 'MATCH (cn:Country)'
        cn_begin_q = True
        if 'params' in req_info['country']:
            if req_info['country']['params']:
                for p in req_info['country']['params']:
                    if p['input_type'] == 'country_codes':
                        country_all_q.append(set_re_match_subquery('cn.country_code', '=~', p['value'], 'string'))
                    if p['input_type'] == 'country_ids':
                        country_all_q.append(set_exact_match_subquery('cn.id', p['value'], 'string'))
                if country_all_q:
                    subquery = build_subquery(country_all_q, 'OR')
                    if not cn_begin_q:
                        country_q += ' AND (' + subquery + ')'
                    else:
                        country_q += ' WHERE (' + subquery + ')'

    if 'province' in req_info:
        p_begin_q = False
        if 'params' in req_info['province']:
            if req_info['province']['params']:
                for p in req_info['province']['params']:
                    if p['input_type'] == 'province_codes':
                        prov_all_q.append(set_re_match_subquery('p.province_code', '=~', p['value'], 'string'))
                    if p['input_type'] == 'province_ids':
                        prov_all_q.append(set_exact_match_subquery('p.id', req_ipnfo['value'], 'string'))
                if prov_all_q:
                    subquery = build_subquery(prov_all_q, 'OR')
                    if not p_begin_q:
                        province_q += ' AND (' + subquery + ')'
                    else:
                        province_q += ' WHERE (' + subquery + ')'

    if 'city' in req_info:
        c_begin_q = False
        if 'params' in req_info['city']:
            if req_info['city']['params']:
                for p in req_info['city']['params']:
                    if p['input_type'] == 'city_names':
                        city_all_q.append(set_re_match_subquery('c.display_name', '=~', p['value'], 'string'))
                    if p['input_type'] == 'city_ids':
                        city_all_q.append(set_exact_match_subquery('c.id', req_ipnfo['value'], 'string'))
                if city_all_q:
                    subquery = build_subquery(city_all_q, 'OR')
                    if not c_begin_q:
                        city_q += ' AND (' + subquery + ')'
                    else:
                        city_q += ' WHERE (' + subquery + ')'

    query = 'MATCH (cn:Country)-[:HAS_PROVINCE]->(p:Province)-[:HAS_CITY]->(c:City)<-[HAS_REGCITY]-(r:Region) ' + country_q + province_q + city_q + ' OPTIONAL MATCH (c)-[:HAS_ZIPCODE]->(z:Zipcode) WITH cn,p,c,z MATCH (c)-[:HAS_BIZ]->(b:Business) WHERE (b.status =~ "(?i)(A)") OPTIONAL MATCH (b)<-[:SOLD_BY]-(s:SubCategory)<-[:INCL_SUBCAT]-(grp:CategoryGroup)<-[:INCL_CATGROUP]-(mc:MainCategory) WITH cn,p,c,z,b, {name: s.display_name} as category WITH cn,p,c,z, {name: b.display_name, category: collect(distinct(category))} as business WITH cn,p,c, {name: z.display_name, business: collect(business)} as zipcode WITH cn,p,{name: c.display_name, zipcode: collect(zipcode)} as city WITH cn,{name: p.display_name, province_code: p.province_code, city: collect(city)} as province WITH {name: cn.display_name, country_code: cn.country_code, province: collect(province)} as country RETURN {country: collect(country)} as country'

    return query

def build_db_query_loc(req_info):

    country_all_q = []
    prov_all_q =    []
    city_all_q =    []
    return_attr =   []
    order_by_q =    []
    country_q = province_q = city_q = ''

    if 'country' in req_info:
        country_q = 'MATCH (cn:Country) WITH cn ORDER BY cn.display_name'
        return_attr.append('COLLECT(cn) as country')
        # order_by_q.append('country.display_name')
        cn_begin_q = True
        if 'params' in req_info['country']:
            if req_info['country']['params']:
                for p in req_info['country']['params']:
                    if p['input_type'] == 'country_codes':
                        country_all_q.append(set_re_match_subquery('cn.country_code', '=~', p['value'], 'string'))
                    if p['input_type'] == 'country_ids':
                        country_all_q.append(set_exact_match_subquery('cn.id', p['value'], 'string'))
                if country_all_q:
                    subquery = build_subquery(country_all_q, 'OR')
                    if not cn_begin_q:
                        country_q += ' AND (' + subquery + ')'
                    else:
                        country_q += ' WHERE (' + subquery + ')'

    if 'province' in req_info:
        province_q = 'MATCH (cn:Country)-[:HAS_PROVINCE]->(p:Province) WITH cn, p ORDER BY p.display_name'
        ## reset
        return_attr = []
        return_attr.append('cn as country')
        return_attr.append('COLLECT(p) as province')
        # order_by_q.append('province.display_name')
        p_begin_q = True
        if 'params' in req_info['province']:
            if req_info['province']['params']:
                for p in req_info['province']['params']:
                    if p['input_type'] == 'province_codes':
                        prov_all_q.append(set_re_match_subquery('p.province_code', '=~', p['value'], 'string'))
                    if p['input_type'] == 'province_ids':
                        prov_all_q.append(set_exact_match_subquery('p.id', req_ipnfo['value'], 'string'))
                if prov_all_q:
                    subquery = build_subquery(prov_all_q, 'OR')
                    if not p_begin_q:
                        province_q += ' AND (' + subquery + ')'
                    else:
                        province_q += ' WHERE (' + subquery + ')'

    if 'city' in req_info:
        city_q = 'MATCH (p:Province)-[:HAS_CITY]->(c:City)'
        # city_q = 'MATCH (p:Province)-[:HAS_CITY]->(c:City) WITH cn, p, c ORDER BY c.display_name'
        ## reset
        return_attr = []
        return_attr.append('cn as country')
        return_attr.append('p as province')
        return_attr.append('COLLECT(c) as city')
        # order_by_q.append('c.display_name')
        c_begin_q = True
        if 'params' in req_info['city']:
            if req_info['city']['params']:
                for p in req_info['city']['params']:
                    if p['input_type'] == 'city_names':
                        city_all_q.append(set_re_match_subquery('c.display_name', '=~', p['value'], 'string'))
                    if p['input_type'] == 'city_ids':
                        city_all_q.append(set_exact_match_subquery('c.id', req_ipnfo['value'], 'string'))
                if city_all_q:
                    subquery = build_subquery(city_all_q, 'OR')
                    if not c_begin_q:
                        city_q += ' AND (' + subquery + ')'
                    else:
                        city_q += ' WHERE (' + subquery + ')'

    query = country_q + ' ' + province_q + ' ' + city_q + ' RETURN ' + r', '.join(return_attr)
     # + ' ORDER BY ' + r', '.join(order_by_q)
    return query

def build_db_query_business(req_info):
    rating_opt = org_opt = neighborhood_opt = cat_opt = region_opt = 'OPTIONAL'
    zipcode_match_q = 'MATCH (c)-[:HAS_ZIPCODE]->(z:Zipcode)-[:HAS_BIZ]->(b:Business)'
    cat_match_q = 'MATCH (s)<-[:INCL_SUBCAT]-(g:CategoryGroup)<-[:INCL_CATGROUP]-(m:MainCategory)'
    neigh_match_q = 'MATCH (n:Neighborhood)-[:HAS_BIZ]->(b:Business)'
    rating_match_q = 'MATCH (b)<-[:HAS_FT_BIZ]-(f:FtRating)'
    org_match_q = 'MATCH (b)<-[:CERTIFIES_BIZ]-(o:Organization)'
    type_code_match_q = 'MATCH (b)<-[:SOLD_BY]-(s:SubCategory)'
    business_match_q = 'MATCH (c)-[:HAS_BIZ]->(b:Business)'
    region_q = org_q = rating_q = neighborhood_q = q_q = type_code_q = business_q = ''
    location_q = cat_q = zipcode_q = ''
    sub_q               = []
    biz_all_q           = []
    country_all_q       = []
    prov_all_q          = []
    region_all_q        = []
    city_all_q          = []
    cat_all_q           = []
    org_all_q           = []
    cat_types_all_q     = []
    org_all_q           = []
    zipcode_all_q       = []
    neigh_all_q         = []
    rating_all_q        = []

    if 'country_codes' in req_info:
        if req_info['country_codes'] is not None:
            country_all_q.append(set_re_match_subquery('cn.country_code', '=~', req_info['country_codes'], 'string'))
    if 'country_ids' in req_info:
        if req_info['country_ids'] is not None:
            country_all_q.append(set_exact_match_subquery('cn.id', req_info['country_ids'], 'string'))
    if country_all_q:
        subquery = build_subquery(country_all_q, 'OR')
        if location_q:
            location_q += ' AND (' + subquery + ')'
        else:
            location_q += ' WHERE (' + subquery + ')'

    if 'province_codes' in req_info:
        if req_info['province_codes'] is not None:
            prov_all_q.append(set_re_match_subquery('p.province_code', '=~', req_info['province_codes'], 'string'))
    if 'province_ids' in req_info:
        if req_info['province_ids'] is not None:
            prov_all_q.append(set_exact_match_subquery('p.id', req_info['province_ids'], 'string'))
    if prov_all_q:
        subquery = build_subquery(prov_all_q, 'OR')
        if location_q:
            location_q += ' AND (' + subquery + ')'
        else:
            location_q += ' WHERE (' + subquery + ')'

    if 'rg_codes' in req_info:
        if req_info['rg_codes'] is not None:
            region_all_q.append(set_re_match_subquery('r.region_code', '=~', req_info['rg_codes'], 'string'))
    if 'rg_ids' in req_info:
        if req_info['rg_ids'] is not None:
            region_all_q.append(set_exact_match_subquery('r.id', req_info['rg_ids'], 'string'))
    if region_all_q:
        subquery = build_subquery(region_all_q, 'OR')
        if location_q:
            location_q += ' AND (' + subquery + ')'
        else:
            location_q += ' WHERE (' + subquery + ')'

    if 'city_names' in req_info:
        if req_info['city_names'] is not None:
            city_all_q.append(set_re_match_subquery('c.display_name', '=~', req_info['city_names'], 'string'))
    if 'city_ids' in req_info:
        if req_info['city_ids'] is not None:
            city_all_q.append(set_exact_match_subquery('c.id', req_info['city_ids'], 'string'))
    if city_all_q:
        subquery = build_subquery(city_all_q, 'OR')
        if location_q:
            location_q += ' AND (' + subquery + ')'
        else:
            location_q += ' WHERE (' + subquery + ')'

    if 'cat_types' in req_info:
        if req_info['cat_types'] is not None:
            cat_types_all_q.append(set_re_subquery('s.type_code', req_info['cat_types'], 'string'))
    if cat_types_all_q:
        type_code_q +=  type_code_match_q + ' WHERE (' + build_subquery(cat_types_all_q, 'OR') + ')'

    if 'zipcodes' in req_info:
        if req_info['zipcodes'] is not None:
            zipcode_all_q.append(set_re_match_subquery('z.display_name', '=~', req_info['zipcodes'], 'string'))
    if zipcode_all_q:
        zipcode_q += zipcode_match_q + ' WHERE (' + build_subquery(zipcode_all_q, 'OR') + ')'

    if 'biz_status' in req_info:
        if req_info['biz_status'] is not None:
            biz_all_q.append(set_re_match_subquery('b.status', '=~', req_info['biz_status'], 'string'))
    if biz_all_q:
        business_q += business_match_q + ' WHERE (' + build_subquery(biz_all_q, 'OR') + ')'

    if 'subcat_names' in req_info:
        if req_info['subcat_names'] is not None:
            cat_all_q.append(set_re_match_subquery('s.display_name', '=~', req_info['subcat_names'], 'string'))
    if 'maincat_names' in req_info:
        if req_info['maincat_names'] is not None:
            cat_all_q.append(set_re_match_subquery('m.display_name', '=~', req_info['maincat_names'], 'string'))
    if cat_all_q:
        cat_q += cat_match_q + ' WHERE (' + build_subquery(cat_all_q, 'OR') + ')'

    if 'org_codes' in req_info:
        if req_info['org_codes'] is not None:
            org_all_q.append(set_re_match_subquery('o.org_code', '=~', req_info['org_codes'], 'string'))
    if org_all_q:
        org_q += org_match_q + ' WHERE (' + build_subquery(org_all_q, 'OR') + ')'
    else:
        org_q = org_opt + ' ' + org_match_q

    if 'neigh_names' in req_info:
        if req_info['neigh_names'] is not None:
            neigh_all_q.append(set_re_subquery('n.display_name', req_info['neigh_names'], 'string'))
    if 'city_ids' in req_info:
        if req_info['city_ids'] is not None:
            neigh_all_q.append(set_exact_match_subquery('c.id', req_info['city_ids'], 'string'))
    if neigh_all_q:
        neighborhood_q += neigh_match_q + ' WHERE (' + build_subquery(neigh_all_q, 'OR') + ')'
    else:
        neighborhood_q = neighborhood_opt + ' ' + neigh_match_q

    if 'ft_min_rating' in req_info:
        if req_info['ft_min_rating'] is not None:
            rating_all_q.append(set_re_match_subquery('f.scale', '>=', req_info['ft_min_rating'], 'number'))
    if rating_all_q:
        rating_q += rating_match_q + ' WHERE (' + build_subquery(rating_all_q, 'OR') + ')'
    else:
        rating_q = rating_opt + ' ' + rating_match_q

    if 'q' in req_info:
        if req_info['q'] is not None or req_info['q'] != '':
            ## double escape is required as part of the RE statement
            q = COMPILED_RE_ESCP.sub(r'\\\\\1', req_info['q'])
            if 'q_filters' in req_info:
                if len(req_info['q_filters']) > 0:
                    for a_type in req_info['q_filters']:
                        if a_type == 'zipcode' or q.isdigit():
                            zipcode_all_q.append(set_re_match_subquery('z.display_name', '=~', q, 'string'))
                            subquery = build_subquery(zipcode_all_q, 'OR')
                            zipcode_q = zipcode_match_q + ' WHERE (' + subquery + ')'
                        elif a_type == 'neighborhood' or a_type == 'locality':
                            neigh_all_q.append(set_re_subquery('n.display_name', q, 'string'))
                            subquery = build_subquery(neigh_all_q, 'OR')
                            neighborhood_q = neigh_match_q + ' WHERE (' + subquery + ')'
                        elif a_type == 'business':
                            sub_q.append(set_re_subquery('b.display_name', q, 'string'))
                        elif a_type == 'category':
                            sub_q.append(set_re_subquery('m.display_name', q, 'string'))
                        elif a_type == 'product':
                            sub_q.append(set_re_subquery('s.tags', q, 'string'))
                            sub_q.append(set_re_subquery('s.display_name', q, 'string'))
                        else:
                            sub_q.append(set_re_subquery('s.tags', q, 'string'))
                            sub_q.append(set_re_subquery('b.display_name', q, 'string'))
                            sub_q.append(set_re_subquery('s.display_name', q, 'string'))
                            sub_q.append(set_re_subquery('m.display_name', q, 'string'))

    if len(sub_q) > 0:
        cat_opt = ''
        q_q = build_subquery(sub_q, 'OR')
        q_q = cat_match_q + ' WHERE (' + q_q + ')'

    query = 'MATCH (cn:Country)-[:HAS_PROVINCE]->(p:Province)-[:HAS_CITY]->(c:City)<-[:HAS_REGCITY]-(r:Region) ' + location_q + ' ' + zipcode_q + ' ' + business_q + ' ' + type_code_q + ' ' + cat_q + ' ' + q_q + ' OPTIONAL MATCH (b)<-[:SOLD_BY]-(sub:SubCategory)<-[:INCL_SUBCAT]-(grp)<-[:INCL_CATGROUP]-(mc) WITH cn, p, c, b, COLLECT(DISTINCT(sub)) as category, COLLECT(DISTINCT(mc)) as main_category ' + neighborhood_q + ' WITH cn, p, c, n as neighborhood, b, category, main_category ' + org_q + ' WITH cn, p, c, neighborhood, b, category,  main_category, COLLECT(o) as organization ' + rating_q + ' WITH cn, p, c, neighborhood, b, category, main_category, organization, collect(f) as ft_rating RETURN cn as country, p as province, c as city, neighborhood, b as business, category, main_category, ft_rating, organization ORDER BY b.display_name LIMIT %s' % CONFIG['neo4j']['rtn_rec_limit']['default']

    return query

def set_re_subquery(name, data, type):
    q = {
        'name': name,
        'op': '=~',
        'data': re.sub(r',', '|', data),
        'data_type': type
    }

    if type == 'string':
        q.update({
            're_template': '(?i).*({data}).*'
        })

    return q

def set_exact_match_subquery(name, data, type):
    q = {
        'name': name,
        'op': '=',
        'data': data,
        'data_type': type
    }

    return q

def set_re_match_subquery(name, op, data, type):
    q = {
        'name': name,
        'op': op,
        'data': re.sub(r',', '|', data),
        'data_type': type
    }

    if type == 'string':
        q.update({
            're_template': '(?i)({data})'
        })

    return q


def respond_records(results):
    r = None
    if results['status_code'] == 'OK':
        rs = None
        if results['result_type'] == 'biz':
            rs = format_biz_records(results['results'])
        elif results['result_type'] == 'loc_cn':
            rs = format_cn_records(results['results'])
        elif results['result_type'] == 'loc_prov':
            rs = format_prov_records(results['results'])
        elif results['result_type'] == 'loc_city':
            rs = format_city_records(results['results'])
        elif results['result_type'] == 'biz_country':
            rs = format_records_by_loc(results['results'])

        r = make_response(jsonify({ 'results': rs }), 200)

    elif results['status_code'] == 'ERR_NOT_FOUND':
        r = make_response(jsonify(
            set_err_response(
                CONFIG['messaging']['error']['210']['msg'],
                CONFIG['messaging']['error']['210']['code']
            )
        ), CONFIG['messaging']['error']['210']['http_code'])
    elif results['status_code'] == 'ERR_DB_SRVC_UNAVAIL':
        r = make_response(jsonify(
            set_err_response(
                CONFIG['messaging']['error']['215']['msg'],
                CONFIG['messaging']['error']['215']['code']
            )
        ), CONFIG['messaging']['error']['215']['http_code'])
    elif results['status_code'] == 'ERR_DB_SESSION_UNAVAIL':
        r = make_response(jsonify(
            set_err_response(
                CONFIG['messaging']['error']['214']['msg'],
                CONFIG['messaging']['error']['214']['code']
            )
        ), CONFIG['messaging']['error']['214']['http_code'])
    else:
        r = make_response(jsonify(
            set_err_response(
                CONFIG['messaging']['error']['208']['msg'],
                CONFIG['messaging']['error']['208']['code']
            )
        ), CONFIG['messaging']['error']['208']['http_code'])

    return r

def exe_query(query):
    r = {}
    try:
        db = get_db_session()
        with db as session:
            with session.begin_transaction() as tx:
                results = tx.run(query)
                ## if a record is there (by peeking), process results
                if results.peek() is not None:
                    r = {
                        'status_code': 'OK',
                        'results': results
                    }
                else:
                    r = { 'status_code': 'ERR_NOT_FOUND' }
    except neo4j.v1.ServiceUnavailable, err:
        #close_db_session(err)
        r = { 'status_code': 'ERR_DB_SRVC_UNAVAIL' }
    except neo4j.v1.SessionError, err:
        #close_db_session(err)
        r = { 'status_code': 'ERR_DB_SESSION_UNAVAIL' }
    except:
        if CONFIG['neo4j']['debug_enabled']['default'] == True:
            traceback.print_exc()
        r = { 'status_code': 'ERR_DB_UNKNOWN'}

    return r

def format_city_records(results):
    ## initialize appropriate response structure
    records = {}
    records.update({
        'records': {
            'country': []
        }
    })

    num_recs = 0
    for rec in results:
        num_recs += len(rec['city'])

        if rec['province']:
            rec['country'].properties.update({
                'province': {
                    'id': rec['province']['id'],
                    'province_code': rec['province']['province_code'],
                    'display_name': rec['province']['display_name'],
                    'latitude': rec['province']['latitude'],
                    'longitude': rec['province']['longitude']
                }
            })

        if rec['city']:
            rec['country'].properties.update({
                'city': [{
                    'id': city['id'],
                    'display_name': city['display_name'],
                    'latitude': city['latitude'],
                    'longitude': city['longitude']
                }  for city in rec['city']]
            })

        records['records']['country'].append(rec['country'].properties)

    records['total_records'] = num_recs
    return records

def format_prov_records(results):
    ## initialize appropriate response structure
    records = {}
    records.update({
        'records': {
            'country': []
        }
    })

    num_recs = 0
    for rec in results:
        if rec['province']:
            num_recs += len(rec['province'])
            rec['country'].properties.update({
                'province': [{
                    'id': prov['id'],
                    'province_code': prov['province_code'],
                    'display_name': prov['display_name'],
                    'latitude': prov['latitude'],
                    'longitude': prov['longitude']
                }  for prov in rec['province']]
            })

        records['records']['country'].append(rec['country'].properties)

    records['total_records'] = num_recs
    return records


def format_cn_records(results):
    ## initialize appropriate response structure
    records = {}
    records.update({
        'records': {}
    })

    num_recs = 0
    for rec in results:
        num_recs += 1
        records['records'].update({
            'country': [{
                'id': country['id'],
                'display_name': country['display_name'],
                'country_code': country['country_code'],
                'latitude': country['latitude'],
                'longitude': country['longitude']
            } for country in rec['country']]
        })

    records['total_records'] = num_recs
    return records

def format_records_by_loc(results):
    ## initialize appropriate response structure
    records = {}
    records.update({
        'records': {}
    })

    num_recs = 0
    for rec in results:
        num_recs += len(rec['country'])
        records['records'].update(
            rec['country']
        )

    records['total_records'] = num_recs
    return records

def format_biz_records(results):
    ## initialize appropriate response structure
    records = {}
    records.update({
        'records': {
            'business': []
        }
    })

    num_recs = 0
    for rec in results:
        num_recs += 1
        if rec['country']:
            # rec['business'].properties.update({ 'country': rec['country'].properties })
            rec['business'].properties.update({ 'country': {
                    'id': rec['country']['id'],
                    'country_code': rec['country']['country_code'],
                    'display_name': rec['country']['display_name']
                }
            })
        if rec['province']:
            rec['business'].properties.update({ 'province': {
                    'id': rec['province']['id'],
                    'province_code': rec['province']['province_code'],
                    'display_name': rec['province']['display_name']
                }
            })
        if rec['city']:
            rec['business'].properties.update({ 'city': {
                        'id': rec['city']['id'],
                        'display_name': rec['city']['display_name'],
                        'latitude': rec['city']['latitude'],
                        'longitude': rec['city']['longitude']
                    }
                })
        if rec['neighborhood']:
            rec['business'].properties.update({ 'neighborhood': {
                    'id': rec['neighborhood']['id'],
                    'display_name': rec['neighborhood']['display_name'],
                    'type': rec['neighborhood']['type']
                }
            })
        ## add other object to the "business" object
        rec['business'].properties.update({
            # 'ft_rating': [ft_rating.properties for ft_rating in rec['ft_rating']],
            'ft_rating': [{
                'id': ft_rating['id'],
                'description': ft_rating['description'],
                'scale': ft_rating['scale']
            }  for ft_rating in rec['ft_rating']],
            'category': [{
                'id': subcat['id'],
                'display_name': subcat['display_name'],
                'tags': subcat['tags'],
                'type_code': subcat['type_code']
            } for subcat in rec['category']],
            'main_category': [{
                'id': maincat['id'],
                'display_name': maincat['display_name'],
                'type': maincat['type']
            } for maincat in rec['main_category']],
            'certification': [{
                'id': cert['id'],
                'display_name': cert['display_name'],
                'contact_email': cert['contact_email'],
                'web_site_url': cert['web_site_url'],
                'latitude': cert['latitude'],
                'longitude': cert['longitude'],
                'org_code': cert['org_code']
            } for cert in rec['organization']]
        })

        records['records']['business'].append(rec['business'].properties)

    records['total_records'] = num_recs
    return records

def build_subquery(data_info, type_q):
    param = q = ''
    q_str = []
    for an_obj in data_info:
        if re.search(',', an_obj['data']):
            data = an_obj['data'].split(',')
            for a_data in data:
                if a_data is not None or a_data is not "":
                    param = a_data
                    if 're_template' in an_obj:
                        param = an_obj['re_template'].replace('{data}', param)
                    if 'data_type' in an_obj:
                        if an_obj['data_type'] == 'string':
                            param = '\"' + param + '\"'
                    q_str.append(an_obj['name'] + " " + an_obj['op'] + " " + param)
        else:
            if 're_template' in an_obj:
                an_obj['data'] = an_obj['re_template'].replace('{data}', an_obj['data'])
            if 'data_type' in an_obj:
                if an_obj['data_type'] == 'string':
                    an_obj['data'] = '\"' + an_obj['data'] + '\"'
            q_str.append(an_obj['name'] + ' ' + an_obj['op'] + ' ' + an_obj['data'])

    type_q = ' ' + type_q + ' '
    q = type_q.join(q_str)
    return q

def set_err_response(err_msg = None, int_err_code = 'err_211'):
    rs_cont = {
        'err_code': int_err_code,
        'err_msg': err_msg
    }
    return rs_cont

def set_debug_info(debug_info, info):
    if not 'debug_info' in debug_info:
        debug_info['debug_info'] = {}
        if not 'proc_flow' in debug_info['debug_info']:
            debug_info['debug_info']['proc_flow'] = []
    #
    # {
    #         'service_name': service_name,
    #         'action': desc,
    #         'ep_url': ep_url,
    #         'elapsed_time': resp_time,
    #         'resp_code': resp_code,
    #         'resp_msg': resp_msg
    #     }

    debug_info['debug_info']['proc_flow'].append(info)

    return debug_info

def is_list_matched(list_1, list_2):
    matched = False
    ## exact match
    if len(set(list_1).intersection(set(list_2))) > 0:
        matched = True

    return matched

def start_time():
    s_time = time.time()
    return s_time

def end_time(s_time):
    e_time = time.time() - s_time
    return e_time

if __name__ == '__main__':
    # load SSL certs if enabled
    if CONFIG['server']['ssl']['enabled']['default'] == True:
        context = (EXE_PATH + CONFIG['server']['ssl']['cert_file']['default'], EXE_PATH + CONFIG['server']['ssl']['key_file']['default'])
        app.run(
            host=CONFIG['server']['host']['default'],
            port=CONFIG['server']['port']['default'],
            ssl_context=context,
            threaded=CONFIG['server']['threaded']['default']
        )
    else:
        app.run(
            host=CONFIG['server']['host']['default'],
            port=CONFIG['server']['port']['default'],
            threaded=CONFIG['server']['threaded']['default']
        )
