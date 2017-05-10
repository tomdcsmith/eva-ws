import argparse
import sys

import numpy as np
from scipy import stats


def main():
    parser = ArgParser(sys.argv)
    ex_times_samp1, ex_times_samp2 = get_ex_times(parser.input_file,
                                                  parser.samp1_name, parser.samp2_name)
    output_stats(ex_times_samp1, ex_times_samp2, parser.samp1_name, parser.samp2_name)


def output_stats(ex_times_samp1, ex_times_samp2, samp1_name, samp2_name):
    samp1_mean = np.mean(ex_times_samp1)
    samp2_mean = np.mean(ex_times_samp2)

    samp1_std = np.std(ex_times_samp1)
    samp2_std = np.std(ex_times_samp2)

    samp1_var = np.var(ex_times_samp1)
    samp2_var = np.var(ex_times_samp2)

    samp1_normality = stats.mstats.normaltest(ex_times_samp1)
    samp2_normality = stats.mstats.normaltest(ex_times_samp2)

    print("find().count():\n{}".format(ex_times_samp1))
    print("aggregate count:\n{}".format(ex_times_samp2))

    print("\nStats [{}, {}]\n".format(samp1_name, samp2_name))
    print("Sample size: {}, {}".format(len(ex_times_samp1), len(ex_times_samp2)))
    print("Normal test: {}, {}\n".format(samp1_normality, samp2_normality))
    print("Mean: {}, {}\n".format(samp1_mean, samp2_mean))
    print("""Percentiles:
min:\t{}\t{}
25:\t{}\t{}
50:\t{}\t{}
75:\t{}\t{}
max:\t{}\t{}\n""".format(
        min(ex_times_samp1), min(ex_times_samp2),
        np.percentile(ex_times_samp1, 25), np.percentile(ex_times_samp2, 25),
        np.percentile(ex_times_samp1, 50), np.percentile(ex_times_samp2, 50),
        np.percentile(ex_times_samp1, 75), np.percentile(ex_times_samp2, 75),
        max(ex_times_samp1), max(ex_times_samp2)
    ))

    print("Standard deviation: {}, {}\n".format(samp1_std, samp2_std))
    print("Variances: {}, {}\n".format(samp1_var, samp2_var))

    if len(ex_times_samp1) > 20 and len(ex_times_samp2) > 20:
        if samp1_normality[1] < 0.05 and samp2_normality[1] < 0.05:
            print(stats.ttest_ind(ex_times_samp1, ex_times_samp2, equal_var=False))

        print(stats.mannwhitneyu(ex_times_samp1, ex_times_samp2))


def get_ex_times(input_file, samp1_name, samp2_name):
    ex_times_fc = []
    ex_times_ac = []
    with open(input_file, "rt") as f:
        for line in f:
            line_list = line.strip().split()
            if line_list[0] == samp1_name:
                ex_times_fc.append(float(line_list[1]))
            elif line_list[0] == samp2_name:
                ex_times_ac.append(float(line_list[1]))
    return ex_times_fc, ex_times_ac


class ArgParser:
    def __init__(self, argv):
        parser = argparse.ArgumentParser()

        parser.add_argument('-i', dest='input_file', action='store', required=True)
        parser.add_argument('-s', dest='samp1_name', action='store', required=True)
        parser.add_argument('-S', dest='samp2_name', action='store', required=True)

        args = parser.parse_args(args=argv[1:])

        self.input_file = args.input_file
        self.samp1_name = args.samp1_name
        self.samp2_name = args.samp2_name


if __name__ == '__main__':
    main()
