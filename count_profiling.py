import argparse
import random
import sys
import traceback
import datetime

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
    "Y": {"minStart": 10003, "maxStart": 59363485, "numEntries": 504508}
}


def main():
    parser = ArgParser(sys.argv)
    collection = get_collection(parser.uri, parser.database, parser.username, parser.password,
                                parser.collection)
    benchmark(collection)


def benchmark(collection):
    n_iters = 6
    numRuns = 3

    chrom = "13"
    minChromPos = 6000000
    maxChromPos = 40000000
    margin = 5000
    query_length = 2000000

    bar = progressbar.ProgressBar()

    benchmark_functions = [benchmark_find_count, benchmark_aggregate_count]

    find_count_ex_times = []
    aggregate_count_ex_times = []

    find_count_counts = []
    aggregate_count_counts = []

    random.shuffle(benchmark_functions)

    print(
        "n_iters: {}, numRuns: {}, chrom: {}, minChromPos: {}, maxChromPos: {}, margin: {}, query_length: {}"
        .format(n_iters, numRuns, chrom, minChromPos, maxChromPos, margin, query_length))

    for _ in bar(list(range(n_iters))):

        benchmark_functions.insert(1, benchmark_functions.pop(0))

        for benchmark_function in benchmark_functions:
            new_ex_times, new_counts = benchmark_function(numRuns, minChromPos, maxChromPos,
                                                          query_length, chrom, margin, collection)
            if benchmark_function == benchmark_find_count:
                find_count_ex_times.extend(new_ex_times)
                find_count_counts.extend(new_counts)
            elif benchmark_function == benchmark_aggregate_count:
                aggregate_count_ex_times.extend(new_ex_times)
                aggregate_count_counts.extend(new_counts)
            else:
                print(benchmark_function)
                sys.exit(1)

        output_stats(find_count_ex_times, find_count_counts, aggregate_count_ex_times,
                     aggregate_count_counts)


def output_stats(find_count_ex_times, find_count_counts, aggregate_count_ex_times,
                 aggregate_count_counts):
    print("\n\n#####################################################\n")
    print("STATS\n")

    print("EX TIMES STATS")
    stats_helper(find_count_ex_times, aggregate_count_ex_times)

    print("\nEX TIMES / RECORD COUNT STATS")
    stats_helper(find_count_counts, aggregate_count_counts)

    print("\n#####################################################\n\n")


def stats_helper(find_count_stat_list, aggregate_count_stat_list):
    find_count_mean = np.mean(find_count_stat_list)
    aggregate_count_mean = np.mean(aggregate_count_stat_list)

    find_count_quartiles = [
        min(find_count_stat_list),
        np.percentile(find_count_stat_list, 25),
        np.percentile(find_count_stat_list, 50),
        np.percentile(find_count_stat_list, 75),
        max(find_count_stat_list)
    ]

    aggregate_count_quartiles = [
        min(aggregate_count_stat_list),
        np.percentile(aggregate_count_stat_list, 25),
        np.percentile(aggregate_count_stat_list, 50),
        np.percentile(aggregate_count_stat_list, 75),
        max(aggregate_count_stat_list)
    ]

    find_count_std = np.std(find_count_stat_list)
    aggregate_count_std = np.std(aggregate_count_stat_list)

    print("find().count():\n{}".format(find_count_stat_list))
    print("aggregate count:\n{}".format(aggregate_count_stat_list))

    print("\nStats [find_count, aggregate_count]\n")
    print("Means: {}, {}".format(find_count_mean, aggregate_count_mean))
    print("Quartiles:\n{}\n{}\n".format(find_count_quartiles, aggregate_count_quartiles))
    print("Standard deviation: {}, {}".format(find_count_std, aggregate_count_std))

    print(
        "T-test (t-statistic, two-tailed p-value): {}".format(stats.ttest_ind(find_count_stat_list,
                                                                              aggregate_count_stat_list,
                                                                              equal_var=False)))


def benchmark_find_count(numRuns, minChromPos, maxChromPos, query_length, chrom, margin,
                         collection):
    ex_times = []
    counts = []
    for _ in range(numRuns):
        # Use random positions for each run to avoid caching effects
        start = random.randint(minChromPos, maxChromPos)
        end = start + query_length

        query = {"chr": chrom,
                 "start": {"$gt": start - margin, "$lte": end},
                 "end": {"$gte": start, "$lte": end + margin}}

        ex_time, count = run_query("find_count", collection, query)

        counts.append(count)
        ex_times.append(ex_time)

    return ex_times, counts


def benchmark_aggregate_count(numRuns, minChromPos, maxChromPos, query_length, chrom, margin,
                              collection):
    ex_times = []
    counts = []
    for _ in range(numRuns):
        # Use random positions for each run to avoid caching effects
        start = random.randint(minChromPos, maxChromPos)
        end = start + query_length

        pipeline = [
            {"$match":
                 {"chr": chrom,
                  "start": {"$gt": start - margin, "$lte": end},
                  "end": {"$gte": start, "$lte": end + margin}}
             },
            {"$group": {
                "_id": None,
                "count": {"$sum": 1}
            }}
        ]

        ex_time, count = run_query("agg_count", collection, pipeline)

        counts.append(count)
        ex_times.append(ex_time)

    return ex_times, counts


def run_query(method, collection, query):
    startTime = datetime.datetime.now()
    if method == "find_count":
        count = collection.find(query).count()
    elif method == "agg_count":
        count = collection.aggregate(query)
    else:
        print("Unrecognised method: {}".format(method))
        sys.exit(1)
    endTime = datetime.datetime.now()
    ex_time = (endTime - startTime).total_seconds()

    return ex_time, count



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


class ArgParser:
    def __init__(self, argv):
        parser = argparse.ArgumentParser()

        parser.add_argument('-i', dest='uri', action='append', help='URI')
        parser.add_argument('-d', dest='database', action='store', help='database')
        parser.add_argument('-l', dest='collection', action='store', help='collection')
        parser.add_argument('-u', dest='username', action='store', help='username')
        parser.add_argument('-p', dest='password', action='store', help='password')

        args = parser.parse_args(args=argv[1:])

        self.uri = args.uri
        self.database = args.database
        self.collection = args.collection
        self.username = args.username
        self.password = args.password


if __name__ == '__main__':
    main()
