#!/usr/bin/python3

import subprocess
import re
import statistics
import matplotlib as mpl
import matplotlib.pyplot as plt
import csv

import numpy as np

import commons
import yaml

csv_file = "data/multi-query-delays.csv"
png_file = "img/multi-query-delays.png"


def run_join(alg, queries, dataset, delay_ms, threads, reps):
    f = open(csv_file, "a")
    results = []

    for i in range(reps):
        s = './app -a ' + str(alg) + ' -d ' + dataset + \
            ' -q ' + str(queries) + ' -p ' + str(delay_ms) + \
            ' -n ' + str(threads) + ' -m 100 '
        stdout = subprocess.check_output(s, cwd="../", shell=True) \
            .decode('utf-8')
        print(stdout)
        for line in stdout.splitlines():
            if "Average throughput" in line:
                throughput = re.findall("\d+\.\d+", line)[1]
                results.append(float(throughput))

    res = statistics.median(results)
    s = (alg + ',' + dataset + ',' + str(queries) + ',' + str(delay_ms) + ',' + str(round(res, 4)))
    print("AVG : " + s)
    f.write(s + "\n")
    f.close()


if __name__ == '__main__':

    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    algs = ['CrkJoin', 'RHO', 'MCJoin', 'PaMeCo']
    MAX_DELAY_MS = 15000
    DELAY_STEP = 500
    QUERIES = 8
    DATASETS = ['A', 'D']

    if config['experiment']:
        commons.remove_file(csv_file)
        commons.remove_file(png_file)
        commons.init_file(csv_file, "alg,dataset,queries,delay,throughput\n")
        commons.compile_app('sgx', config='Enclave/Enclave4GB.config.xml',
                            application='MULTI_QUERY')
        for alg in algs:
            print("Run " + alg)
            for ds in DATASETS:
                for delay in range(DELAY_STEP, MAX_DELAY_MS + 1, DELAY_STEP):
                    run_join(alg, QUERIES, ds, delay, config['threads'][alg], config['reps'])

    # commons.plot_concurrent_queries()
