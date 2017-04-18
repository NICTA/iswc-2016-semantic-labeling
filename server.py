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
from shutil import copyfile

# logging
logFormatter = logging.Formatter("%(asctime)s [%(name)-12.12s] [%(levelname)-10.10s]  %(message)s")
rootLogger = logging.getLogger()
rootLogger.setLevel(logging.INFO)

fileHandler = logging.FileHandler('karma-server.log', mode='w')
fileHandler.setFormatter(logFormatter)
fileHandler.setLevel(logging.INFO)
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
es_tracer.setLevel(logging.INFO)
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
domains = ["soccer", "dbpedia", "museum", "weather"]


@service.errorhandler(404)
def not_found(error=None):
    message = {
            'status': 404,
            'message': 'Not Found: ' + request.url,
    }
    resp = jsonify(message)
    resp.status_code = 404
    return resp


@service.errorhandler(414)
def bad_uri(err=None):
    logging.error("Bad uri {}: {}.".format(request.url, err))
    message = {
            'status': 414,
            'message': err + " <in> " + request.url,
    }
    resp = jsonify(message)
    resp.status_code = 414
    return resp


def error(message=""):
    with service.app_context():
        print("Error message: ", message)
        response = make_response()
        response.status_code = 500
        response.headers = {
            "X-Status-Reason": message,
            "message": message
        }
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
    try:
        if not (column and semantic_type):
            column = request.json["column"]
            semantic_type = request.json["semantic_type"]
        logging.info("Adding semantic type: {}".format(semantic_type))
        column_name = column.keys()[0]

        if column and semantic_type and column_name:
            source = Source(column_name)
            source.read_data_from_dict(column)
            source.set_semantic_type(semantic_type, column_name)
            _id = get_new_index_name(semantic_type, column_name)
            source.save(index_config={"name": _id, "size": 0})
            resp = jsonify({"index_name": _id})
            resp.status_code = 200
            return resp
    except Exception as e:
        return error("Semantic type adding failed: {}".format(e.args))


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

    if "header" not in request.json or "values" not in request.json:
        return bad_uri("Either header, or source, or values not in request.")
    header = request.json["header"]
    values = request.json["values"]
    if "source" not in request.json:
        source=None
    else:
        source = request.json["source"]

    try:
        column = Column(header, source)
        for element in values:
            logging.info("Add element: {}".format(element))
            logging.debug("column: {}".format(column))
            column.add_value(element)

        return str(semantic_labeler.predict_semantic_type_for_column(column))
    except Exception as e:
        logging.error("Get semantic type: {}".format(e))
        return error("Getting semantic type failed: {}".format(e.args))


@service.route(FIRST_TIME_URL, methods=["GET"])
def first_time():
    logging.info("First time setup...")
    try:
        semantic_labeler.reset()
        semantic_labeler.read_data_sources(["museum"])
        # semantic_labeler.train_semantic_types(["soccer"])
        # semantic_labeler.train_random_forest([11], ["soccer"])

        # semantic_labeler.read_data_sources(["soccer", "dbpedia", "museum", "weather"])
        # semantic_labeler.train_semantic_types(["soccer", "dbpedia", "museum", "weather"])
        # semantic_labeler.train_random_forest([11], ["soccer"])

        # semantic_labeler.read_data_sources(["soccer", "dbpedia", "museum","flights", "weather", "phone"])
        # semantic_labeler.train_semantic_types(["soccer", "dbpedia", "museum", "flights", "weather", "phone"])
        # semantic_labeler.train_random_forest([11], ["soccer"])
        semantic_labeler.write_data_sources(limit=None, filter_unknown=False)
        resp = jsonify("Training complete.")
        resp.status_code = 200
        return resp
    except Exception as e:
        logging.error("First time setup: {}".format(e))
        return error(str(e.args[0]) + " "+str(e.args))


@service.route(RESET_URL, methods=["POST"])
def reset_semantic_labeler():
    """
    This endpoint is needed to clean elastic search server, reset model and all read in data sources.
    It is crucial to reset elastic search before retraining.
    """
    logging.info("Resetting the labeler")
    try:
        semantic_labeler.reset()
        resp = jsonify("DSL reset!")
        resp.status_code = 200
        return resp
    except Exception as e:
        logging.error("Semantic labeler reset: {}".format(e.args))
        return error("Semantic labeler reset failed: {}".format(e.args))


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
        logging.error("Test: {}".format(e.args))
        return error("Test failed due to: "+str(e.args[0])+" "+str(e.args))


@service.route('/domain', methods=["POST"])
def read_folder():
    """
    Index domain with data sources with semantic labeler.
    :return:
    """
    if "folder" not in request.json:
        return bad_uri("missing parameter: 'folder' not in request")
    folder_name = request.json["folder"]
    logging.info("Indexing data sources from folder {}".format(folder_name))
    try:
        semantic_labeler.read_data_sources(folder_name)
        logging.info("Listing folders for response.")
        return list_folder()
    except Exception as e:
        logging.error("Indexing data sources: {}".format(e))
        return error("Folder indexing failed due to: "+str(e.args[0])+" "+str(e.args))


@service.route('/folder', methods=["GET"])
def list_folder():
    """
    List available folders with data sources on the server.
    :return:
    """
    logging.info("Listing folders")
    resp = jsonify({"folder_names": list(semantic_labeler.dataset_map.keys())})
    resp.status_code = 200
    # TODO: implement
    return resp


@service.route(SEMANTIC_TYPE_URL, methods=["GET"])
def train_semantic_types():
    """
    Train semantic types for a list of folders.
    :return:
    """
    if request.json is None:
        return bad_uri("JSON missing")
    if "folder" not in request.json:
        return bad_uri("missing parameter: 'folder' not in request")
    folders = request.json["folder"]
    if not(isinstance(folders, list)):
        return bad_uri("wrong parameter: 'folder' not list")
    logging.info("Training semantic types for {}".format(folders))
    semantic_labeler.train_semantic_types(folders)
    resp = jsonify("Semantic types trained.")
    resp.status_code = 200
    # TODO: implement
    return resp


@service.route('/train', methods=["POST"])
def train_logistic_regression():
    """
    Train logistic regression:
        train_sizes
        folder_names
    :return:
    """
    if request.json is None:
        return bad_uri("JSON missing")
    if "folder" not in request.json or "size" not in request.json:
        return bad_uri("missing parameter: 'folder' or/and 'size' not in request")
    folders = request.json["folder"]
    if not(isinstance(folders, list)):
        return bad_uri("wrong parameter: 'folder' not list")
    train_sizes = request.json["size"]
    if not(isinstance(train_sizes, list)):
        return bad_uri("wrong parameter: 'size' not list")
    try:
        logging.info("Training logistic regression for {}".format(folders))
        semantic_labeler.train_random_forest(train_sizes, folders)
        resp = jsonify("Logistic regression trained.")
        resp.status_code = 200
        # TODO: implement
        return resp
    except Exception as e:
        logging.error("Training: {}".format(e))
        return error("Training failed due to: {}".format(e))


@service.route('/predict', methods=["GET"])
def predict_logistic_regression():
    """
    Predict for all sources in the folder specified in the request json:
        folder
    The specified folder must be indexed by the semantic labeler already.
    :return:
    """
    if request.json is None:
        return bad_uri("JSON missing")
    if "folder" not in request.json:
        return bad_uri("missing parameter: 'folder' not in request")
    folders = request.json["folder"]
    logging.info("Predicting semantic types for {}".format(folders))
    ## rather run predict per each data source separately!!!
    try:
        result = semantic_labeler.predict_folder_semantic_types(folders)
        resp = jsonify(result)
        resp.status_code = 200
        return resp
    except Exception as e:
        logging.error("Prediction for the folder failed: {}".format(e))
        return error("Prediction for the folder failed due to: {}".format(e))


@service.route('/copy', methods=["POST"])
def copy_data():
    """
    Create folder with the specified name which holds sources specified in the request.
    Sources to be put into the folder should already exist on the server within a domain folder.
    Domain folders are fixed and listed in the global variable domains.
    We just copy necessary files from the domain folders into the specified folder.
    This method is useful to create train and test data folders.
    :return:
    """
    logging.info("Creating new folder")
    if request.json is None:
        return bad_uri("JSON missing")
    if "folder" not in request.json or "files" not in request.json:
        return bad_uri("missing parameter: 'folder' or/and 'files' not in request")
    requested_folder_name = request.json["folder"]
    requested_file_names = request.json["files"]
    if not(isinstance(requested_file_names, list)):
        return bad_uri("wrong parameter: 'files' not list")
    try:

        # creating folder: it should contain subfolders data and model
        new_folder = os.path.join(semantic_labeler.data_folder, requested_folder_name)
        folder_sources = os.path.join(new_folder, "data")
        folder_models = os.path.join(new_folder, "model")
        if not(os.path.exists(new_folder)):
            logging.info("Creating folder: {}".format(new_folder))
            os.makedirs(new_folder)
        if not(os.path.exists(folder_sources)):
            os.makedirs(folder_sources)
            logging.info("Creating folder: {}".format(folder_sources))
        else:
            # delete all sources from the folder
            logging.info("Cleaning {}".format(folder_sources))
            [os.remove(os.path.join(folder_sources, f)) for f in os.listdir(folder_sources)]
        if not(os.path.exists(folder_models)):
            logging.info("Creating folder: {}".format(folder_models))
            os.makedirs(folder_models)
        else:
            # delete all models from the folder
            [os.remove(os.path.join(folder_models, f)) for f in os.listdir(folder_models)]
            logging.info("Cleaning {}".format(folder_models))

        copied_files = 0
        for folder_name in domains:
            folder_path = os.path.join(semantic_labeler.data_folder, folder_name)
            data_folder_path = os.path.join(folder_path, "data")
            model_folder_path = os.path.join(folder_path, "model")
            for filename in os.listdir(data_folder_path):
                if filename in requested_file_names:
                    src = os.path.join(data_folder_path, filename)
                    dst = os.path.join(folder_sources, filename)
                    logging.info("Coping source {}".format(src))
                    # we copy source to the train folder
                    copyfile(src, dst)
                    if os.path.exists(model_folder_path):
                        src = os.path.join(model_folder_path, filename+".model.json")
                        dst = os.path.join(folder_models, filename+".model.json")
                        logging.info("Coping model {}".format(src))
                        # we copy model to the train folder
                        copyfile(src, dst)
                    copied_files += 1
        logging.info("Indexing new folder: {}".format(requested_folder_name))
        semantic_labeler.read_data_sources([requested_folder_name])

        resp = jsonify({"new_folder": requested_folder_name, "copied_sources": copied_files})
        resp.status_code = 200
        return resp
    except Exception as e:
        logging.error("Creating new folder: {}".format(e))
        return error("Folder creation failed due to: {}".format(e))


if __name__ == "__main__":
    service.run(debug=True, port=8000, host="0.0.0.0")

