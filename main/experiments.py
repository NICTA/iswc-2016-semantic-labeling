import random

import time

import sys

from importlib import reload

from main.semantic_labeler import SemanticLabeler
# from semantic_labeler import SemanticLabeler

__author__ = 'alse'

import logging
import os

# logging
logFormatter = logging.Formatter("%(asctime)s [%(name)-12.12s] [%(levelname)-10.10s]  %(message)s")
rootLogger = logging.getLogger()
rootLogger.setLevel(logging.INFO)

fileHandler = logging.FileHandler('karma-experiments.log', mode='w')
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
es_logger_handler = logging.handlers.RotatingFileHandler('karma-experiments-elasticsearch-base.log',
                                                      maxBytes=0.5*10**9,
                                                      backupCount=3, mode='w')
es_logger.addHandler(es_logger_handler)

es_tracer = logging.getLogger('elasticsearch.trace')
es_tracer.propagate = False
es_tracer.setLevel(logging.DEBUG)
es_tracer_handler = logging.handlers.RotatingFileHandler('karma-experiments-elasticsearch-full.log',
                                                   maxBytes=0.5*10**9,
                                                   backupCount=3, mode='w')
es_tracer.addHandler(es_tracer_handler)

# logging for py4j
py4j_logger = logging.getLogger('py4j')
py4j_logger.propagate = False
py4j_logger.setLevel(logging.INFO)
py4j_logger_handler = logging.handlers.RotatingFileHandler('karma-experiments-py4j.log',
                                                   maxBytes=0.5*10**9,
                                                   backupCount=3, mode='w')
py4j_logger.addHandler(py4j_logger_handler)


def run_experiments():
    semantic_labeler = SemanticLabeler(data_folder="/home/natalia//PycharmProjects/karma_semantic_typer/iswc-2016-semantic-labeling/data/datasets")
    semantic_labeler.reset()
    semantic_labeler.read_data_sources(["dbpedia", "soccer", "museum", "weather"])
    semantic_labeler.train_semantic_types(["dbpedia", "soccer", "museum", "weather"])
    # semantic_labeler.train_semantic_types(["museum"])
    # semantic_labeler.train_semantic_types(["weather2"])
    # start_time = time.time()
    # semantic_labeler.train_random_forest([1, 2, 3, 4, 5], ["soccer"])
    # print("--- %s seconds ---" % (time.time() - start_time))
    #
    # semantic_labeler.train_random_forest([1], ["soccer"])
    # semantic_labeler.train_random_forest([1], ["museum"])
    # print("--- %s seconds ---" % (time.time() - start_time))

    sizes = random.sample(range(1, 12), 2)

    semantic_labeler.train_random_forest([1], ["soccer"])
    # semantic_labeler.train_random_forest([1], ["museum2"])
    #
    # semantic_labeler.test_semantic_types("museum2", [14])

    # semantic_labeler.test_semantic_types("dbpedia", [5])
    # semantic_labeler.test_semantic_types("museum", [14])
    # semantic_labeler.test_semantic_types("soccer", [6])
    # semantic_labeler.test_semantic_types("weather", [2])
    semantic_labeler.test_semantic_types("dbpedia", [1, 2, 3, 4, 5])
    semantic_labeler.test_semantic_types("museum", [1, 2, 3, 4, 5])
    semantic_labeler.test_semantic_types("soccer", [1, 2, 3, 4, 5])
    semantic_labeler.test_semantic_types("weather", [1, 2, 3])
    # semantic_labeler.test_semantic_types("weather", [2])
    # semantic_labeler.test_semantic_types("soccer", [6])
    # semantic_labeler.test_semantic_types("soccer", [6])
    # semantic_labeler.test_semantic_types("soccer", [6])
    # semantic_labeler.test_semantic_types("soccer", [6])
    # semantic_labeler.test_semantic_types("weather", [2])
    # semantic_labeler.test_semantic_types("weather", xrange(1, 3))
    # semantic_labeler.test_semantic_types_from_2_sets("dbpedia_full", "t2d")


if __name__ == "__main__":
    # reload(sys)
    # sys.setdefaultencoding('utf-8') ## no longer available in py3
    run_experiments()
