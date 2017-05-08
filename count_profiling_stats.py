import argparse
import sys

import numpy as np
from scipy import stats


def main():
    parser = ArgParser(sys.argv)
    ex_times_fc, ex_times_ac = get_ex_times(parser.input_file)
    output_stats(ex_times_fc, ex_times_ac)


def output_stats(ex_times_fc, ex_times_ac):
    fc_mean = np.mean(ex_times_fc)
    ac_mean = np.mean(ex_times_ac)

    fc_quartiles = [
        np.percentile(ex_times_fc, 25),
        np.percentile(ex_times_fc, 50),
        np.percentile(ex_times_fc, 75)
    ]

    ac_quartiles = [
        np.percentile(ex_times_ac, 25),
        np.percentile(ex_times_ac, 50),
        np.percentile(ex_times_ac, 75)
    ]

    fc_std = np.std(ex_times_fc)
    ac_std = np.std(ex_times_ac)

    fc_var = np.var(ex_times_fc)
    ac_var = np.var(ex_times_ac)

    fc_normality = stats.mstats.normaltest(ex_times_fc)
    ac_normality = stats.mstats.normaltest(ex_times_ac)

    print("find().count():\n{}".format(ex_times_fc))
    print("aggregate count:\n{}".format(ex_times_ac))

    print("\nStats [find_count, aggregate_count]\n")
    print("Normal test: {}, {}\n".format(fc_normality, ac_normality))
    print("Mean: {}, {}\n".format(fc_mean, ac_mean))
    print("""Percentiles:
min:\t{}\t{}
25:\t{}\t{}
50:\t{}\t{}
75:\t{}\t{}
max:\t{}\t{}\n""".format(
        min(ex_times_fc), min(ex_times_ac),
        np.percentile(ex_times_fc, 25), np.percentile(ex_times_ac, 25),
        np.percentile(ex_times_fc, 50), np.percentile(ex_times_ac, 50),
        np.percentile(ex_times_fc, 75), np.percentile(ex_times_ac, 75),
        max(ex_times_fc), max(ex_times_ac)
    ))

    print("Standard deviation: {}, {}\n".format(fc_std, ac_std))
    print("Variances: {}, {}\n".format(fc_var, ac_var))

    if len(ex_times_fc) > 20 and len(ex_times_ac) > 20:
        if fc_normality[1] < 0.05 and ac_normality[1] < 0.05:
            print(stats.ttest_ind(ex_times_fc, ex_times_ac, equal_var=False))
        else:
            print(stats.mannwhitneyu(ex_times_fc, ex_times_ac))


def get_ex_times(input_file):
    ex_times_fc = []
    ex_times_ac = []
    with open(input_file, "rt") as f:
        for line in f:
            line_list = line.strip().split()
            if line_list[0] == "fc":
                ex_times_fc.append(float(line_list[1]))
            elif line_list[0] == "ac":
                ex_times_ac.append(float(line_list[1]))
    return ex_times_fc, ex_times_ac

class ArgParser:
    def __init__(self, argv):
        parser = argparse.ArgumentParser()

        parser.add_argument('-i', dest='input_file', action='store', required=True)

        args = parser.parse_args(args=argv[1:])

        self.input_file = args.input_file


if __name__ == '__main__':
    main()
