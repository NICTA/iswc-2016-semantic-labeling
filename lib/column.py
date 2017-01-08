import locale
import re
from collections import defaultdict
import logging

from numpy import percentile, array
from numpy.random import choice

from lib.utils import split_number_text, not_allowed_chars, get_distribution
from tests.integrated import get_test_results

__author__ = 'alse'


class Column:
    def __init__(self, name, source_name=None):
        logging.debug("Initializing column: {}".format((name, source_name)))
        self.source_name = source_name
        self.name = name.replace("#", "")
        self.value_list = []
        self.textual_list = []
        self.textual_set = set()
        self.word_set = set()
        self.semantic_type = None
        self.numeric_list = []
        self.sample_list = []
        self.value_text = ""
        self.is_prepared = False
        self.word2vec = []
        self.word_lengths = []
        self.char_lengths = []
        self.histogram_list = []

    def __str__(self):
        return "<Column: name= " + str(self.name) + ", source= " + str(self.source_name) + ">"

    def __repr__(self):
        return self.__str__()

    def add_value(self, value):
        if not value:
            return

        value = value.strip()
        # logging.debug("  adding value (" + str(value) + ") to column (" + str(self.name) + ") with type " + str(type(value)))
        if not value or value == "NULL":
            return
        # String handling is different in Python3 from what it was in Python2
        # All strings are by default unicode
        value = value.encode("ascii", "ignore").decode()
        # try:
        #     value = value.encode("ascii", "ignore").decode()
        #     logging.debug("  decoding: {}".format(type(value)))
        # except:
        #     value = value.decode("unicode_escape").encode("ascii", "ignore").decode("ascii", "ignore")
        #     logging.debug("  decoding: {}".format(type(value)))

        value = re.sub(not_allowed_chars, " ", value)
        # logging.debug("    1 subsitute not allowed chars: {}".format(value))

        self.word_set = self.word_set.union(set(value.split(" ")))
        # logging.debug("    2 ...")

        if "full" in self.source_name and len(self.value_list) > 500:
            return
        # logging.debug("    2 ...")
        self.value_list.append(value)
        # logging.debug("    2 ...")

        self.word_lengths.append(len(value.split(" ")))
        # logging.debug("    2 ...")
        self.char_lengths.append(len(value))
        # logging.debug("    2 ...")

        numbers, text = split_number_text(value)
        # logging.debug("    3 ...")

        if text:
            self.value_text += (" " + text)

            self.textual_set.add(text)
            self.textual_list.append(text)
        # logging.debug("    4 splitting")
        if numbers:
            self.numeric_list.append(max([locale.atof(v[0]) for v in numbers]))
        # logging.debug("      value added.")

    def prepare_data(self):
        self.word2vec = []
        # for word in self.word_set:
        #     try:
        #         self.word2vec.append(np.asarray(word2vec[word]))
        #     except:
        #         continue
        # self.word2vec = np.mean(np.asarray(self.word2vec), axis=0).tolist()
        if not isinstance(self.word2vec, list):
            self.word2vec = []
        if not self.is_prepared:
            sample_size = min(200, len(self.numeric_list))
            # print self.value_list
            if percentile(array(self.word_lengths), 25) != percentile(array(self.word_lengths), 75):
                self.word_lengths = []
            if percentile(array(self.char_lengths), 25) != percentile(array(self.char_lengths), 75):
                self.char_lengths = []

            self.histogram_list = get_distribution(self.value_list)
            if len(self.histogram_list) > 20:
                self.histogram_list = []
            # print self.histogram_list

            if self.numeric_list:
                self.sample_list = choice(self.numeric_list, sample_size).tolist()
            else:
                self.sample_list = self.numeric_list
            self.is_prepared = True

    def to_json(self):
        logging.debug("Column to json: {}".format(self.name))
        self.prepare_data()
        doc_body = {'source': self.source_name,
                    'name': self.name,
                    'semantic_type': self.semantic_type,
                    'textual_set': list(self.textual_set),
                    "textual_list": self.textual_list,
                    "values": self.value_list,
                    'sample_list': self.sample_list,
                    'textual': self.value_text,
                    'is_numeric': self.is_numeric(),
                    'word2vec': self.word2vec,
                    'numeric_list': self.numeric_list,
                    'char_lengths': self.char_lengths,
                    "word_lengths": self.word_lengths,
                    "histogram": self.histogram_list}
        return doc_body

    def read_json_to_column(self, json_obj):
        logging.debug("Reading json to column")
        self.name = json_obj['name']
        logging.debug("  -- {}".format(self.name))
        self.semantic_type = json_obj['semantic_type']
        self.value_list = json_obj['values']
        self.histogram_list = json_obj['histogram']
        self.numeric_list = json_obj['numeric']
        self.numeric_count = len(self.numeric_list)
        self.sample_list = json_obj['sample_numeric']
        self.value_text = json_obj['textual']

    def is_numeric(self):
        logging.debug("Column check for numeric: {}".format(self.name))
        return len(self.textual_list) * 1.0 / (len(self.textual_list) + len(self.numeric_list))

    def predict_type(self, train_examples_map, textual_train_map, model):
        """

        :param train_examples_map:
        :param textual_train_map:
        :param model:
        :return:
        """
        logging.debug("Predicting type for column: {}".format(self.name))
        feature_vectors = self.generate_candidate_types(train_examples_map, textual_train_map)
        predictions = model.predict(feature_vectors, self.semantic_type)
        predictions = [
            ((round(prediction['prob'], 2), prediction['prob'], self.source_name + "!" + self.name),
             prediction['name'].split("!")[0])
            for prediction in predictions]
        prediction_map = defaultdict(lambda: [])
        for prediction in predictions:
            prediction_map[prediction[0][0]].append(prediction[1])
        return sorted(list(prediction_map.items()), reverse=True)

    def generate_candidate_types(self, train_examples_map, textual_train_map, is_labeled=False):
        logging.debug("Generating types for column: {}".format(self.name))
        return get_test_results(train_examples_map, textual_train_map, self.to_json(), is_labeled)
