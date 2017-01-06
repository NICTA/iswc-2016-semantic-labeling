from flask import Flask, request
from flask import make_response
from flask import jsonify

from lib import indexer
from lib.column import Column
from lib.source import Source
from lib.utils import get_new_index_name
from main.semantic_labeler import SemanticLabeler

import logging
import os

# logging
logFormatter = logging.Formatter("%(asctime)s [%(name)-12.12s] [%(levelname)-10.10s]  %(message)s")
rootLogger = logging.getLogger()
rootLogger.setLevel(logging.DEBUG)

fileHandler = logging.FileHandler('karma-server.log', mode='w')
fileHandler.setFormatter(logFormatter)
fileHandler.setLevel(logging.DEBUG)
rootLogger.addHandler(fileHandler)

consoleHandler = logging.StreamHandler()
consoleHandler.setLevel(logging.WARNING)
consoleHandler.setFormatter(logFormatter)
rootLogger.addHandler(consoleHandler)

# logging for elasticsearch
es_logger = logging.getLogger('elasticsearch')
es_logger.propagate = False
es_logger.setLevel(logging.INFO)
es_logger_handler = logging.handlers.RotatingFileHandler('karma-elasticsearch-base.log',
                                                      maxBytes=0.5*10**9,
                                                      backupCount=3, mode='w')
es_logger.addHandler(es_logger_handler)

es_tracer = logging.getLogger('elasticsearch.trace')
es_tracer.propagate = False
es_tracer.setLevel(logging.DEBUG)
es_tracer_handler = logging.handlers.RotatingFileHandler('karma-elasticsearch-full.log',
                                                   maxBytes=0.5*10**9,
                                                   backupCount=3, mode='w')
es_tracer.addHandler(es_tracer_handler)

# logging for py4j
py4j_logger = logging.getLogger('py4j')
py4j_logger.propagate = False
py4j_logger.setLevel(logging.INFO)
py4j_logger_handler = logging.handlers.RotatingFileHandler('karma-py4j.log',
                                                   maxBytes=0.5*10**9,
                                                   backupCount=3, mode='w')
py4j_logger.addHandler(py4j_logger_handler)

# logger = logging.getLogger('mainLog')
# logger.propagate = False
# logger.setLevel(logging.DEBUG)
# # create file handler
# fileHandler = logging.handlers.RotatingFileHandler('elasticsearclk.log',
#                                                maxBytes=10**6,
#                                                backupCount=3)
# fileHandler.setLevel(logging.INFO)
# # create console handler
# consoleHandler = logging.StreamHandler()
# consoleHandler.setLevel(logging.INFO)
# # create formatter and add it to the handlers
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# consoleHandler.setFormatter(formatter)
# fileHandler.setFormatter(formatter)
# # add the handlers to logger
# logger.addHandler(consoleHandler)
# logger.addHandler(fileHandler)


# logging.basicConfig(filename='app.log',
#                     level=logging.DEBUG, filemode='w',
#                     format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

__author__ = 'alse'

service = Flask(__name__)

SEMANTIC_TYPE_URL = "/semantic_type"
COLUMN_URL = "/column"
FIRST_TIME_URL = "/ftu"
UPLOAD_FOLDER = "/data/"  # TODO change this to model folder
TEST_URL = "/test"
RESET_URL = "/reset"
semantic_labeler = SemanticLabeler()


def error(message=""):
    with service.app_context():
        print("Error message: ", message)
        response = make_response()
        response.status_code = 500
        response.headers = {
            "X-Status-Reason": message
        }
        print("Error response: ", response)
        return response


def allowed_file(filename, extensions):
    return '.' in filename and \
           filename.rsplit('.', 1)[1] in extensions

@service.route('/')
def hello():
    resp = jsonify("Karma DSL running here!")
    resp.status_code = 200
    return resp

@service.route(SEMANTIC_TYPE_URL, methods=['POST', 'PUT'])
def add_semantic_type(column=None, semantic_type=None):
    if not (column and semantic_type):
        column = request.json["column"]
        semantic_type = request.json["semantic_type"]


    column_name = column.keys()[0]

    if column and semantic_type and column_name:
        source = Source(column_name)
        source.read_data_from_dict(column)
        source.set_semantic_type(semantic_type, column_name)
        _id = get_new_index_name(semantic_type, column_name)
        source.save(index_config={"name": _id, "size": 0})
        return str(_id)
        """
    try:
        if not (column and semantic_type):
            column = request.json["column"]
            semantic_type = request.json["semantic_type"]
        column_name = column.keys()[0]

        if column and semantic_type and column_name:
            source = Source(column_name)
            source.read_data_from_dict(column)
            source.set_semantic_type(semantic_type, column_name)
            _id = get_new_index_name(semantic_type, column_name)
            source.save(index_config={"name": _id, "size": 0})
            return str(_id)
    except Exception as e:
        return error(e.message+" "+str(e.args))"""


@service.route(SEMANTIC_TYPE_URL, methods=["DELETE"])
def delete_semantic_type():
    semantic_type = request.json["semantic_type"]
    _id = get_new_index_name(semantic_type, "*")
    if not indexer.delete_column(index_config={"name": _id, "size": 0}):
        logging.error("Unable to delete semantic type.")
        return error("Unable to delete semantic type.")
    resp = jsonify("Deleted semantic type " + str(semantic_type))
    resp.status_code = 200
    return resp


# NOTE. This is to add many columns to a given semantic type in a bulk
@service.route(SEMANTIC_TYPE_URL + "/bulk", methods=['POST'])
def add_semantic_type_bulk():
    semantic_type = request.json["semantic_type"]
    columns = request.json["columns"]

    for column in columns:
        add_semantic_type(column, semantic_type)

    resp = jsonify("Bulk adding semantic type " + str(semantic_type))
    resp.status_code = 200
    return resp


@service.route(COLUMN_URL, methods=["DELETE"])
def delete_column():
    logging.info("Deleting column...")
    semantic_type = request.json["semantic_type"]
    column_name = request.json["column_name"]
    _id = get_new_index_name(semantic_type, column_name)
    if not indexer.delete_column(index_config={"name": _id, "size": 0}):
        logging.error("Unable to delete semantic type.")
        return error("Unable to delete semantic type.")
    resp = jsonify("Column deleted: " + str(column_name))
    resp.status_code = 200
    return resp

@service.route(COLUMN_URL, methods=["POST"])
def get_semantic_type():
    logging.info("Getting semantic type...")
    try:
        print(request)
        header = request.json["header"]
        source = request.json["source"]
        values = request.json["values"]

        column = Column(header, source)
        for element in values:
            logging.info("Add element: {}".format(element))
            column.add_value(element)

        return str(semantic_labeler.predict_semantic_type_for_column(column))
    except Exception as e:
        logging.error("Get semantic type: {}".format(e))
        return error(str(e))


@service.route(FIRST_TIME_URL, methods=["GET"])
def first_time():
    logging.info("First time setup...")
    try:
        semantic_labeler.reset()
        # semantic_labeler.read_data_sources(["soccer"])
        # semantic_labeler.train_semantic_types(["soccer"])
        # semantic_labeler.train_random_forest([11], ["soccer"])

        semantic_labeler.read_data_sources(["soccer", "dbpedia", "museum", "weather"])
        semantic_labeler.train_semantic_types(["soccer", "dbpedia", "museum", "weather"])
        semantic_labeler.train_random_forest([11], ["soccer"])

        # semantic_labeler.read_data_sources(["soccer", "dbpedia", "museum","flights", "weather", "phone"])
        # semantic_labeler.train_semantic_types(["soccer", "dbpedia", "museum", "flights", "weather", "phone"])
        # semantic_labeler.train_random_forest([11], ["soccer"])
        semantic_labeler.write_data_sources()
        resp = jsonify("Training complete.")
        resp.status_code = 200
        return resp
    except Exception as e:
        logging.error("First time setup: {}".format(e))
        return error(str(e.args[0]) + " "+str(e.args))

@service.route(RESET_URL, methods=["POST"])
def reset_semantic_labeler():
    semantic_labeler.reset()

@service.route(TEST_URL, methods=["GET"])
def test_service():
    logging.info("Running test")
    try:
        semantic_labeler.read_data_sources(["soccer", "weather"])
        semantic_labeler.train_random_forest([5], ["soccer"])
        semantic_labeler.test_semantic_types("weather", [3])
        logging.info("Test complete")
        resp = jsonify("Tests complete")
        resp.status_code=200
        return resp
    except Exception as e:
        logging.error("Test: {}".format(e))
        return error("Test failed due to: "+str(e.args[0])+" "+str(e.args))

if __name__ == "__main__":
    service.run(debug=True, port=8000)

