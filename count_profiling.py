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

    random.shuffle(benchmark_functions)

    for _ in bar(list(range(n_iters))):

        benchmark_functions.insert(1, benchmark_functions.pop(0))

        for benchmark_function in benchmark_functions:
            new_ex_times = benchmark_function(numRuns, minChromPos, maxChromPos, query_length, chrom, margin, collection)
            if benchmark_function == benchmark_find_count:
                find_count_ex_times.extend(new_ex_times)
            elif benchmark_function == benchmark_aggregate_count:
                aggregate_count_ex_times.extend(new_ex_times)
            else:
                print(benchmark_function)
                sys.exit(1)

        output_stats(find_count_ex_times, aggregate_count_ex_times)




def output_stats(find_count_ex_times, aggregate_count_ex_times):
    find_count_mean = np.mean(find_count_ex_times)
    aggregate_count_mean = np.mean(aggregate_count_ex_times)

    find_count_quartiles = [
        min(find_count_ex_times),
        np.percentile(find_count_ex_times, 25),
        np.percentile(find_count_ex_times, 50),
        np.percentile(find_count_ex_times, 75),
        max(find_count_ex_times)
    ]

    aggregate_count_quartiles = [
        min(aggregate_count_ex_times),
        np.percentile(aggregate_count_ex_times, 25),
        np.percentile(aggregate_count_ex_times, 50),
        np.percentile(aggregate_count_ex_times, 75),
        max(aggregate_count_ex_times)
    ]

    find_count_std = np.std(find_count_ex_times)
    aggregate_count_std = np.std(aggregate_count_ex_times)

    print("\n\n#####################################################\n")
    print("STATS\n")

    print("find().count() execution times: {}".format(find_count_ex_times))
    print("aggregate count execution times: {}".format(aggregate_count_ex_times))

    print("\nStats [find_count, aggregate_count]\n")
    print("Means: {}, {}".format(find_count_mean, aggregate_count_mean))
    print("Quartiles:\n{}\n{}\n".format(find_count_quartiles, aggregate_count_quartiles))
    print("Standard deviation: {}, {}".format(find_count_std, aggregate_count_std))

    print("T-test (t-statistic, two-tailed p-value): {}".format(stats.ttest_ind(find_count_ex_times,
                                                                                aggregate_count_ex_times,
                                                                                equal_var=False)))
    print("\n#####################################################\n\n")


def benchmark_find_count(numRuns, minChromPos, maxChromPos, query_length, chrom, margin, collection):
    ex_times = []
    for _ in range(numRuns):
        # Use random positions for each run to avoid caching effects
        start = random.randint(minChromPos, maxChromPos)
        end = start + query_length

        query = {"chr": chrom,
                 "start": {"$gt": start - margin, "$lte": end},
                 "end": {"$gte": start, "$lte": end + margin}}

        startTime = datetime.datetime.now()
        collection.find(query).count()
        endTime = datetime.datetime.now()
        ex_times.append((endTime - startTime).total_seconds())

    return ex_times


def benchmark_aggregate_count(numRuns, minChromPos, maxChromPos, query_length, chrom, margin, collection):
    ex_times = []
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

        startTime = datetime.datetime.now()
        collection.aggregate(pipeline)
        endTime = datetime.datetime.now()
        ex_times.append((endTime - startTime).total_seconds())

    return ex_times


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
