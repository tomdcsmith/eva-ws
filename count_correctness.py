import argparse
import random

import sys

import progressbar
from bson.codec_options import CodecOptions
from pymongo import MongoClient, ReadPreference


CHROM_INFO = {
    "1": {"minStart": 10020, "maxStart": 249240605, "numEntries": 12422239},
    "2": {"minStart": 10133, "maxStart": 243189190, "numEntries": 13217397},
    "3": {"minStart": 60069, "maxStart": 197962381, "numEntries": 10891260},
    "4": {"minStart": 10006, "maxStart": 191044268, "numEntries": 10427984},
    "5": {"minStart": 10043, "maxStart": 180905164, "numEntries": 9742153},
    "6": {"minStart": 61932, "maxStart": 171054104, "numEntries": 9340928},
    "7": {"minStart": 10010, "maxStart": 159128653, "numEntries": 8803393},
    "8": {"minStart": 10059, "maxStart": 146303974, "numEntries": 8458842},
    "9": {"minStart": 10024, "maxStart": 141153428, "numEntries": 6749462},
    "10": {"minStart": 60222, "maxStart": 135524743, "numEntries": 7416994},
    "11": {"minStart": 61248, "maxStart": 134946509, "numEntries": 7690584},
    "12": {"minStart": 60076, "maxStart": 133841815, "numEntries": 7347630},
    "13": {"minStart": 19020013, "maxStart": 115109865, "numEntries": 5212835},
    "14": {"minStart": 19000005, "maxStart": 107289456, "numEntries": 4989875},
    "15": {"minStart": 20000003, "maxStart": 102521368, "numEntries": 4607392},
    "16": {"minStart": 60008, "maxStart": 90294709, "numEntries": 5234679},
    "17": {"minStart": 47, "maxStart": 81195128, "numEntries": 4652428},
    "18": {"minStart": 10005, "maxStart": 78017157, "numEntries": 4146560},
    "19": {"minStart": 60360, "maxStart": 59118925, "numEntries": 3821659},
    "20": {"minStart": 60039, "maxStart": 62965384, "numEntries": 3512381},
    "21": {"minStart": 9411199, "maxStart": 48119868, "numEntries": 2082680},
    "22": {"minStart": 16050036, "maxStart": 51244515, "numEntries": 2172028},
    "X": {"minStart": 60003, "maxStart": 155260479, "numEntries": 5893713},
    # "Y": {"minStart": 10003, "maxStart": 59363485, "numEntries": 504508}
}


def main():
    parser = ArgParser(sys.argv)
    collection = PymongoConnection(parser.uri, parser.database, parser.username, parser.password,
                                   parser.collection)
    check_counts(collection, parser.margin, parser.query_length, parser.runs)


def check_counts(collection, margin, query_length, runs):
    benchmark_functions = ["fc", "ac"]
    random.shuffle(benchmark_functions)

    bar = progressbar.ProgressBar()
    for _ in bar(list(range(runs))):
        benchmark_functions.insert(1, benchmark_functions.pop(0))

        chromosome = random.choice(list(CHROM_INFO.keys()))
        min_pos, max_pos = get_min_max_pos(chromosome, query_length)
        start = random.randint(min_pos, max_pos)
        end = start + query_length

        print("{}\t{}\t{}\t{}".format(chromosome, start, end, margin))

        fc_count = None
        ac_count = None

        for benchmark_function in benchmark_functions:
            if benchmark_function == "fc":
                query = {"chr": chromosome,
                         "start": {"$gt": start - margin, "$lte": end},
                         "end": {"$gte": start, "$lte": end + margin}}

                fc_count = run_query("find_count", collection, query)

            elif benchmark_function == "ac":
                query = [
                    {"$match":
                         {"chr": chromosome,
                          "start": {"$gt": start - margin, "$lte": end},
                          "end": {"$gte": start, "$lte": end + margin}}
                     },
                    {"$group": {
                        "_id": None,
                        "count": {"$sum": 1}
                    }}
                ]

                ac_count = run_query("agg_count", collection, query)

            else:
                print(benchmark_function)
                sys.exit(1)

        assert fc_count == ac_count, "fc_count ({}) != ac_count ({}) for query: {}".format(fc_count,
                                                                                           ac_count,
                                                                                           query)


def run_query(method, collection, query):
    while True:
        try:
            if method == "find_count":
                count = collection.collection.find(query).count()
            elif method == "agg_count":
                cursor = collection.collection.aggregate(query)
                count = cursor.next()["count"]
            else:
                print("Unrecognised method: {}".format(method))
                sys.exit(1)

            return count

        except StopIteration:
            # print("Stop iteration, trying again")
            # print("query: {}".format(query))
            collection.get_client()
            collection.get_collection()


def get_min_max_pos(chromosome, query_length):
    min_pos = CHROM_INFO[chromosome]["minStart"]
    max_pos = CHROM_INFO[chromosome]["maxStart"] - query_length
    return min_pos, max_pos


class PymongoConnection:
    def __init__(self, uri=None, db=None, user=None, password=None, collection=None):
        self.uri = uri
        self.db = db
        self.user = user
        self.password = password
        self.collection_name = collection

        self.client = self.get_client()
        self.collection = self.get_collection()

    def get_collection(self):
        db = self.client[self.db]
        options = CodecOptions(unicode_decode_error_handler="ignore")
        self.collection = db.get_collection(self.collection_name, codec_options=options)
        return self.collection

    def get_client(self):
        self.client = MongoClient(self.uri, port=27017, read_preference=ReadPreference.SECONDARY_PREFERRED)
        self.client.admin.authenticate(self.user, self.password)
        return self.client


class ArgParser:
    def __init__(self, argv):
        parser = argparse.ArgumentParser()

        parser.add_argument('-i', dest='uri', action='append', help='URI')
        parser.add_argument('-d', dest='database', action='store', help='database')
        parser.add_argument('-l', dest='collection', action='store', help='collection')
        parser.add_argument('-u', dest='username', action='store', help='username')
        parser.add_argument('-p', dest='password', action='store', help='password')
        parser.add_argument('-m', dest='margin', action='store', help='margin')
        parser.add_argument('-q', dest='query_length', action='store', help='query_length')
        parser.add_argument('-r', dest='runs', action='store', help='runs')

        args = parser.parse_args(args=argv[1:])

        self.uri = args.uri
        self.database = args.database
        self.collection = args.collection
        self.username = args.username
        self.password = args.password
        self.margin = int(args.margin)
        self.query_length = int(args.query_length)
        self.runs = int(args.runs)


if __name__ == '__main__':
    main()
