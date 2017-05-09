import argparse
import random
import sys
import traceback
import datetime
import math

import progressbar
from bson.codec_options import CodecOptions
from pymongo import MongoClient, ReadPreference
import numpy as np
from scipy import stats


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
    benchmark(collection, parser.margin, parser.query_length, parser.passes)


def benchmark(collection, margin, query_length, passes):
    # fc_ex_times = []
    # ac_ex_times = []
    #
    # fc_counts = []
    # ac_counts = []
    #
    # chromosomes = [str(chromosome) for chromosome in range(1, 23)] + ["X"]
    # # chromosomes = ["X", "Y"]

    benchmark_functions = ["fc", "ac"]
    chromosomes = list(CHROM_INFO.keys())

    while True:
        random.shuffle(benchmark_functions)

        # bar = progressbar.ProgressBar()
        for benchmark_function in benchmark_functions:
            chromosome = random.choice(chromosomes)
            min_pos, max_pos = get_min_max_pos(chromosome, query_length)
            start = random.randint(min_pos, max_pos)
            end = start + query_length

            if benchmark_function == "fc":
                query = {"chr": chromosome,
                         "start": {"$gt": start - margin, "$lte": end},
                         "end": {"$gte": start, "$lte": end + margin}}

                ex_time, count, startTime = run_query("find_count", collection, query)
                # fc_ex_times.append(ex_time)
                # fc_counts.append(count)

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

                ex_time, count, startTime = run_query("agg_count", collection, query)
                # ac_ex_times.append(ex_time)
                # ac_counts.append(count)

            else:
                print(benchmark_function)
                sys.exit(1)

            with open("ex_times/ex_times_{}.tsv".format(query_length), "at") as f:
                out_list = [benchmark_function,
                            ex_time,
                            count,
                            startTime.strftime("%Y/%m/%d_%H:%M:%S:%f"),
                            chromosome,
                            start,
                            end,
                            margin,
                            "\n"]
                out_list = [str(item) for item in out_list]
                out_string = '\t'.join(out_list)
                f.write(out_string)


def query_to_str(chromosome, start, end, margin):
    return "{}:{}-{}:{}".format(chromosome, start, end, margin)


def get_min_max_pos(chromosome, query_length):
    min_pos = CHROM_INFO[chromosome]["minStart"]
    max_pos = CHROM_INFO[chromosome]["maxStart"] - query_length
    return min_pos, max_pos


def output_stats(fc_ex_times, fc_counts, ac_ex_times, ac_counts):

    rec_div = 10000

    fc_time_per_n_rec = np.divide(fc_ex_times, np.divide(fc_counts, rec_div))
    ac_time_per_n_rec = np.divide(ac_ex_times, np.divide(ac_counts, rec_div))

    print("\n\n#####################################################\n")
    print("STATS\n")

    print("EX TIMES STATS")
    stats_helper(fc_ex_times, ac_ex_times)

    print("\nTIME PER {} RECORDS".format(rec_div))
    stats_helper(fc_time_per_n_rec, ac_time_per_n_rec)

    print("\n#####################################################\n\n")


def stats_helper(fc_stat_list, ac_stat_list):
    fc_mean = np.mean(fc_stat_list)
    ac_mean = np.mean(ac_stat_list)

    fc_quartiles = [
        min(fc_stat_list),
        np.percentile(fc_stat_list, 25),
        np.percentile(fc_stat_list, 50),
        np.percentile(fc_stat_list, 75),
        max(fc_stat_list)
    ]

    ac_quartiles = [
        min(ac_stat_list),
        np.percentile(ac_stat_list, 25),
        np.percentile(ac_stat_list, 50),
        np.percentile(ac_stat_list, 75),
        max(ac_stat_list)
    ]

    fc_std = np.std(fc_stat_list)
    ac_std = np.std(ac_stat_list)

    print("find().count():\n{}".format(fc_stat_list))
    print("aggregate count:\n{}".format(ac_stat_list))

    print("\nStats [find_count, aggregate_count]\n")
    print("Means: {}, {}".format(fc_mean, ac_mean))
    print("Quartiles:\n{}\n{}\n".format(fc_quartiles, ac_quartiles))
    print("Standard deviation: {}, {}".format(fc_std, ac_std))

    if len(fc_stat_list) > 20 and len(ac_stat_list) > 20:
        print("Mann-Whitney rank test (test statistic, pvalue): {}".format(
            stats.mannwhitneyu(fc_stat_list, ac_stat_list)))


def run_query(method, collection, query):
    while True:
        try:
            if method == "find_count":
                startTime = datetime.datetime.now()
                count = collection.collection.find(query).count()
                endTime = datetime.datetime.now()
            elif method == "agg_count":
                startTime = datetime.datetime.now()
                cursor = collection.collection.aggregate(query)
                endTime = datetime.datetime.now()
                count = cursor.next()["count"]
            else:
                print("Unrecognised method: {}".format(method))
                sys.exit(1)

            ex_time = (endTime - startTime).total_seconds()

            return ex_time, count, startTime
        except StopIteration:
            # print("Stop iteration, trying again")
            # print("query: {}".format(query))
            # sys.exit(1)
            collection.get_client()
            collection.get_collection()


def get_collection(uri=None, db=None, user=None, password=None, collection=None):
    try:
        client = MongoClient(uri, port=27017, read_preference=ReadPreference.SECONDARY_PREFERRED)
        client.admin.authenticate(user, password)
        db = client[db]
        options = CodecOptions(unicode_decode_error_handler="ignore")
        coll = db.get_collection(collection, codec_options=options)
        return coll
    except:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        e = sys.exc_info()[0]
        print("Error: %s\tline: %s\texc_obj: %s" % (e, exc_tb.tb_lineno, exc_obj))
        traceback.print_tb(exc_tb)
        sys.exit(1)


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
        parser.add_argument('-P', dest='passes', action='store', help='passes')

        args = parser.parse_args(args=argv[1:])

        self.uri = args.uri
        self.database = args.database
        self.collection = args.collection
        self.username = args.username
        self.password = args.password
        self.margin = int(args.margin)
        self.query_length = int(args.query_length)
        self.passes = int(args.passes)


if __name__ == '__main__':
    main()
