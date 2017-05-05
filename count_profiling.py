import argparse
import getpass
import sys

import traceback
from pymongo import MongoClient, ReadPreference
from bson.codec_options import CodecOptions


def main():
    parser = ArgParser(sys.argv)
    collection = get_collection(parser.uri, parser.database, parser.username, parser.password,
                                parser.collection)
    print(collection.find_one())


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



# mongoProdClient = MongoClient(guiutils.promptGUIInput("Host", "Host"))
# mongoProdUname = guiutils.promptGUIInput("User", "User")
# mongoProdPwd = guiutils.promptGUIInput("Pass", "Pass", "*")
# mongoProdDBHandle = mongoProdClient["admin"]
# mongoProdDBHandle.authenticate(mongoProdUname, mongoProdPwd)
# mongoProdDBHandle = mongoProdClient["eva_hsapiens_grch37"]
#
# mongoProdCollHandle = mongoProdDBHandle["variants_1_1"]
# mongoProdCollHandle_2 = mongoProdDBHandle["variants_1_2"]
#
# for i in range(0,5):
#
#     numRuns = 30
#     cumulativeExecTime = 0
#     minChromPos= 6000000
#     maxChromPos = 100000000
#     margin = 1000000
#     for i in range(0,numRuns):
#         # Use random positions for each run to avoid caching effects
#         pos = random.randint(minChromPos, maxChromPos)
#         startTime = datetime.datetime.now()
#         marginScanResultList = list(mongoProdCollHandle.find({"chr":"10",  "start": {"$gt": pos - margin},"start": {"$lte": pos + margin}, "end": {"$gte": pos}, "end": {"$lt": pos + margin + margin}}).sort([("chr", pymongo.ASCENDING), ("start", pymongo.ASCENDING)]).limit(1000))
#         endTime = datetime.datetime.now()
#         cumulativeExecTime += (endTime - startTime).total_seconds()
#     print("Average Execution Time with Margin Scan for variants_1_1: {0}".format (str(cumulativeExecTime/numRuns)))
#
#
#     cumulativeExecTime = 0
#     for i in range(0,numRuns):
#         pos = random.randint(minChromPos, maxChromPos)
#         startTime = datetime.datetime.now()
#         nonMarginScanResultList = list(mongoProdCollHandle.find({"chr":"10", "start": {"$gte": pos},"start": {"$lte": pos + margin}, "end": {"$gte": pos}, "end": {"$lte": pos + margin}}).sort([("chr", pymongo.ASCENDING), ("start", pymongo.ASCENDING)]).limit(1000))
#         endTime = datetime.datetime.now()
#         cumulativeExecTime += (endTime - startTime).total_seconds()
#     print("Average Execution Time with No Margin Scan for variants_1_1: {0}".format (str(cumulativeExecTime/numRuns)))
#
#
#     numRuns = 30
#     cumulativeExecTime = 0
#     minChromPos= 50000000
#     maxChromPos = 100000000
#     margin = 1000000
#     for i in range(0,numRuns):
#         # Use random positions for each run to avoid caching effects
#         pos = random.randint(minChromPos, maxChromPos)
#         startTime = datetime.datetime.now()
#         marginScanResultList = list(mongoProdCollHandle_2.find({"chr":"15", "start": {"$gt": pos - margin},"start": {"$lte": pos + margin}, "end": {"$gte": pos}, "end": {"$lt": pos + margin + margin}}).sort([("chr", pymongo.ASCENDING), ("start", pymongo.ASCENDING)]).limit(1000))
#         endTime = datetime.datetime.now()
#         cumulativeExecTime += (endTime - startTime).total_seconds()
#     print("Average Execution Time with Margin Scan for variants_1_2: {0}".format (str(cumulativeExecTime/numRuns)))
#
#
#     cumulativeExecTime = 0
#     for i in range(0,numRuns):
#         pos = random.randint(minChromPos, maxChromPos)
#         startTime = datetime.datetime.now()
#         nonMarginScanResultList = list(mongoProdCollHandle_2.find({"chr":"15", "start": {"$gte": pos},"start": {"$lte": pos + margin}, "end": {"$gte": pos}, "end": {"$lte": pos + margin}}).sort([("chr", pymongo.ASCENDING), ("start", pymongo.ASCENDING)]).limit(1000))
#         endTime = datetime.datetime.now()
#         cumulativeExecTime += (endTime - startTime).total_seconds()
#     print("Average Execution Time with No Margin Scan for variants_1_2: {0}".format (str(cumulativeExecTime/numRuns)))
#     print("****************************************************************************************")


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
