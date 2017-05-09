import csv
import json
import re
from xml.etree import ElementTree
import logging
import random

from .column import Column
from lib import indexer
from lib.utils import not_allowed_chars

__author__ = 'alse'


class Source(object):

    @staticmethod
    def find_source_encoding(file_path, limit=100):
        """
        Helper function to determine encoding of the file.
        For now 5 encodings are checked.
        :param file_path: Path of the file.
        :param limit: How many lines will be read in to determine the encoding.
        :return:
        """
        encodings = ["utf-8", "utf-16", "iso-8859-1", "latin-1", "windows-1252"]
        for enc in encodings:
            try:
                with open(file_path, "r", encoding=enc) as f:
                    [f.readline() for _ in range(limit)]
                return enc
            except:
                continue
        logging.error("Correct encoding was not found: {}".format(file_path))
        return

    def __init__(self, name):
        logging.debug("Initializing source: {}".format(name))
        self.name = name
        self.index_name = re.sub(not_allowed_chars, "", self.name)
        self.column_map = {}

    def __str__(self):
        return "<Source: " + str(self.name) + ">"

    def __repr__(self):
        return self.__str__()

    def save(self, index_config):
        logging.info("Saving source: {}".format(self.name))
        indexer.index_source(source=self, index_config=index_config)

    def write_column_map(self, filepath, filter_unknown=False):
        logging.info("Writing column map to {}".format(filepath))
        with open(filepath, "w+") as f:
            if filter_unknown:
                column_map = [(k, v) for (k, v) in self.column_map.items()
                              if v.semantic_type is not None]
                column_map = dict(column_map)
            else:
                column_map = self.column_map
            writer = csv.writer(f, quotechar='"', quoting=csv.QUOTE_ALL)
            writer.writerow(["key", "column_name", "source_name", "semantic_type"])
            for key, col in column_map.items():
                writer.writerow([key, col.name, col.source_name, col.semantic_type])

    def read_semantic_type_json(self, file_path):
        enc = self.find_source_encoding(file_path)
        with open(file_path, 'r', encoding=enc) as f:
            logging.info("Reading semantic type json: {}".format(file_path))
            data = json.load(f)
            node_array = data["graph"]["nodes"]

            for node in node_array:
                if "userSemanticTypes" in node:
                    semantic_object = node["userSemanticTypes"]
                    name = node["columnName"]
                    self.set_semantic_type(semantic_object[0], name)
                    # domain = semantic_object[0]["domain"]["uri"].split("/")[-1]
                    # type = semantic_object[0]["type"]["uri"].split("/")[-1]
                    # self.column_map[name].semantic_type = domain + "---" + type

    def set_semantic_type(self, semantic_object, name):
        logging.info("Setting semantic type: {}".format(name))
        domain = semantic_object["domain"]["uri"].split("/")[-1]
        _type = semantic_object["type"]["uri"].split("/")[-1]
        try:
            self.column_map[name.replace(" ", "")].semantic_type = domain + "---" + _type
        except:
            return

    def write_csv_file(self, file_path, limit=500, filter_unknown=False):
        with open(file_path, "w") as csv_file:
            logging.info("Writing csv: {}".format(file_path))
            writer = csv.writer(csv_file, quotechar='"', quoting=csv.QUOTE_ALL)
            if filter_unknown:
                column_map = [(k, v) for (k, v) in self.column_map.items()
                              if v.semantic_type is not None]
                column_map = dict(column_map)
            else:
                column_map = self.column_map
            try:
                headers = [column.name for column in column_map.values()]
                writer.writerow(headers)
            except:
                logging.warning("No headers for dataset: {}".format(file_path))
            if limit is None:
                limit = max([len(column.value_list) for column in column_map.values()])
            for i in range(limit):
                output_list = []
                for column in column_map.values():
                    try:
                        output_list.append(column.value_list[i])
                    except:
                        output_list.append("")
                writer.writerow(output_list)

    def read_data_from_dict(self, data):
        logging.info("Reading data from dict: {}".format(self.name))
        for header in data.keys():
            self.column_map[header] = Column(header)
            for element in data[header]:
                self.column_map[header].add_value(element)

    def read_data_from_csv(self, file_path):
        correct_encoding = self.find_source_encoding(file_path)
        with open(file_path, encoding=correct_encoding) as csv_file:
            logging.info("Reading data from csv: {} ".format(file_path))
            reader = csv.DictReader(csv_file)
            headers = reader.fieldnames
            for header in headers:
                if header:
                    header = header.replace(" ", "")
                    self.column_map[header] = Column(header, file_path)
                    #for weather 2 data
                    # self.column_map[header].semantic_type = header

            for row in reader:
                for header in row.keys():
                    if header:
                        self.column_map[header.replace(" ", "")].add_value(row[header])

    def read_data_from_wc_csv(self, file_path):
        correct_encoding = self.find_source_encoding(file_path)
        with open(file_path, encoding=correct_encoding) as csv_file:
            logging.info("Reading data from wc csv: {}".format(file_path))
            reader = csv.DictReader(csv_file)
            headers = reader.fieldnames
            for header in headers:
                header = header.replace(" ", "")
                self.column_map[header] = Column(header, file_path)

            idx = 0
            for row in reader:
                if idx == 0:
                    for header in self.column_map.keys():
                        if "ontology" not in row[header]:
                            del self.column_map[header]
                        else:
                            self.column_map[header].semantic_type = row[header]
                    idx = 1
                    continue
                else:
                    for header in self.column_map.keys():
                        # if "http://" in row[header]:
                        #     self.column_map[header].add_value(row[header].split("/")[-1].replace("_", " "))
                        # else:
                        self.column_map[header].add_value(row[header])

    def read_data_from_json(self, file_path):
        enc = self.find_source_encoding(file_path)
        with open(file_path, 'r', encoding=enc) as f:
            logging.info("Reading data from json: {}".format(file_path))
            json_array = json.load(f)
            for node in json_array:
                for field in node.keys():
                    if field not in self.column_map:
                        column = Column(field, file_path)
                        self.column_map[field] = column
                    if isinstance(node[field], list):
                        for value in node[field]:
                            self.column_map[field].add_value(str(value))
                    elif isinstance(node[field], dict):
                        for field1 in node[field].keys():
                            if field1 not in self.column_map:
                                column = Column(field1, file_path)
                                self.column_map[field1] = column
                            self.column_map[field1].add_value(str(node[field][field1]))
                    else:
                        self.column_map[field].add_value(str(node[field]))

    def read_data_from_xml(self, file_path):
        correct_encoding = self.find_source_encoding(file_path)
        with open(file_path, 'r', encoding=correct_encoding) as f:
            logging.info("Reading data from xml: {}".format(file_path))
            xml_tree = ElementTree.parse(f)
            root = xml_tree.getroot()
            for child in root:
                for attrib_name in child.attrib.keys():
                    if attrib_name not in self.column_map:
                        column = Column(attrib_name, file_path)
                        self.column_map[attrib_name] = column
                    self.column_map[attrib_name].add_value(child.attrib[attrib_name])
                for attrib in child:
                    if attrib.tag not in self.column_map:
                        column = Column(attrib.tag, file_path)
                        self.column_map[attrib.tag] = column
                    self.column_map[attrib.tag].add_value(attrib.text)

    def read_semantic_type_from_gold(self, file_path):
        logging.info("Reading semantic type from gold: {}".format(file_path))
        correct_encoding = self.find_source_encoding(file_path)
        with open(file_path, 'r', encoding=correct_encoding) as f:
            csv_reader = csv.reader(f)
            for row in csv_reader:
                if len(row) > 1 and row[1].strip() in self.column_map:
                    self.column_map[row[1].strip()].semantic_type = row[0].strip()

    def read_data_from_text_file(self, file_path):
        logging.info("Reading data from text file: {}".format(file_path))
        correct_encoding = self.find_source_encoding(file_path)
        with open(file_path, 'r', encoding=correct_encoding) as f:
            num_types = int(f.readline())
            # headers are randomly sampled numbers from (1000,9999)
            up = (10**4)-1
            down = 10**3
            headers = random.sample(range(down, up), num_types)
            f.readline()
            for num_type in range(num_types):
                semantic_type = f.readline().strip()
                column = Column(str(headers[num_type]), file_path)
                column.semantic_type = "---".join(
                    [part.split("/")[-1] for part in semantic_type.replace("#", "").split("|")])
                num_values = int(f.readline())
                for num_val in range(num_values):
                    column.add_value(f.readline().split(" ", 1)[1])
                f.readline()
                self.column_map[column.name] = column
