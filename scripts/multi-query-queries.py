#!/usr/bin/python3

import subprocess
import re
import statistics
import sys

import matplotlib.pyplot as plt
import csv

import numpy as np
from matplotlib.lines import Line2D
from matplotlib.patches import Rectangle

import commons
import yaml

csv_file = "data/multi-query-queries.csv"
png_file = "img/multi-query-queries.png"


def run_join(alg, queries, dataset, threads, reps):
    f = open(csv_file, "a")
    results = []

    for i in range(reps):
        s = './app -a ' + str(alg) + ' -m 100 -d ' + str(dataset) + \
            ' -q ' + str(queries) + ' -p 0 ' + ' -n ' + str(threads)
        stdout = subprocess.check_output(s, cwd="../",shell=True)\
            .decode('utf-8')
        for line in stdout.splitlines():
            if "Average throughput" in line:
                throughput = re.findall("\d+\.\d+", line)[1]
                results.append(float(throughput))

    res = statistics.median(results)
    s = (alg + ',' + str(dataset) + ',' + str(queries) + ',' + str(round(res,4)))
    print("AVG : " + s)
    f.write(s + "\n")
    f.close()


# def plot():
#     csvf = open(csv_file, mode='r')
#     csvr = csv.DictReader(csvf)
#     all_data = list(csvr)
#     delays = sorted(set(map(lambda x: int(x['delay']), all_data)))
#     crkj_data = list(filter(lambda x:x['alg'] == 'CRKJ', all_data))
#     crkj_to_delays = [[y for y in crkj_data if int(y['delay']) == x] for x in delays]
#
#     rho_data = list(filter(lambda x:x['alg'] == 'RHO', all_data))
#     rho_to_delays = [[y for y in rho_data if int(y['delay']) == x] for x in delays]
#
#     mcjoin_data = list(filter(lambda x:x['alg'] == 'MCJoin', all_data))
#     mcjoin_to_delays = [[y for y in mcjoin_data if int(y['delay']) == x] for x in delays]
#
#     fig = plt.figure(figsize=(3,3))
#
#     # print improvement over RHO
#     for d in range(len(delays)):
#         delay = delays[d]
#         crkj = crkj_to_delays[d]
#         rho = rho_to_delays[d]
#         crkj_queries = list(map(lambda x: int(x['queries']), crkj))
#         crkj_throughput = list(map(lambda x: float(x['throughput']), crkj))
#         rho_queries = list(map(lambda x: int(x['queries']), rho))
#         rho_throughput = list(map(lambda x: float(x['throughput']), rho))
#         l = len(crkj_queries)
#         if len(crkj_queries) != len(rho_queries):
#             l = min(len(crkj_queries), len(rho_queries))
#         improvement = np.divide(crkj_throughput[:l], rho_throughput[:l])
#         plt.plot(crkj_queries[:l], improvement, color = commons.color_categorical(d),
#                  linewidth=1, marker='^', markeredgecolor=commons.color_categorical(d),
#                  markersize=6, markeredgewidth=0.5, label='RHO-'+str(delay))
#
#     # print improvement over MCJoin
#     for d in range(len(delays)):
#         delay = delays[d]
#         crkj = crkj_to_delays[d]
#         mcjoin = mcjoin_to_delays[d]
#         crkj_queries = list(map(lambda x: int(x['queries']), crkj))
#         crkj_throughput = list(map(lambda x: float(x['throughput']), crkj))
#         mcjoin_queries = list(map(lambda x: int(x['queries']), mcjoin))
#         mcjoin_throughput = list(map(lambda x: float(x['throughput']), mcjoin))
#         l = len(crkj_queries)
#         if len(crkj_queries) != len(mcjoin_queries):
#             l = min(len(crkj_queries), len(mcjoin_queries))
#         improvement = np.divide(crkj_throughput[:l], mcjoin_throughput[:l])
#         plt.plot(crkj_queries[:l], improvement, color = commons.color_categorical(d),
#                  linewidth=1, marker='o', markeredgecolor=commons.color_categorical(d),
#                  markersize=6, markeredgewidth=0.5, label='MCJoin-'+str(delay))
#
#     # make the figure neat
#     plt.ylabel("Throughput improvement factor")
#     plt.xlabel('Total number of queries')
#     plt.yscale('log')
#     plt.gca().yaxis.grid(linestyle='dashed')
#     # plt.gca().set_axisbelow(True)
#     # fig.text(0.5, 0.8, 'delay = ' + str(delay_ms) + ' ms', ha='center')
#     lines, labels = fig.axes[-1].get_legend_handles_labels()
#     lines.clear()
#     labels.clear()
#
#     handles = [Rectangle((0, 0), 1, 1, fc="w", fill=False, edgecolor='none', linewidth=0, label='Time between queries:'),
#                Line2D([0], [0], color=commons.color_categorical(0),
#                       linewidth=2, label='100ms'),
#                Line2D([0], [0], color=commons.color_categorical(1),
#                       linewidth=2, label='200ms'),
#                Line2D([0], [0], color=commons.color_categorical(2),
#                       linewidth=2, label='300ms'),
#                Rectangle((0, 0), 1, 1, fc="w", fill=False, edgecolor='none', linewidth=0, label=''),
#                Rectangle((0, 0), 1, 1, fc="w", fill=False, edgecolor='none', linewidth=0, label='Improvement factor:'),
#                Line2D([], [], color='black', marker='^', linestyle='None',
#                       markersize=6, label='over RHO'),
#                Line2D([], [], color='black', marker='o', linestyle='None',
#                       markersize=6, label='over MCJoin')
#                ]
#
#     fig.legend(handles=handles, fontsize='small', frameon=0,
#                ncol=1, bbox_to_anchor=(0.93, 0.45), loc='lower left',
#                borderaxespad=0, handletextpad=0.25, columnspacing=1.5)
#     commons.savefig(png_file)


if __name__ == '__main__':

    config = commons.read_config(sys.argv)

    algs = ['CrkJoin', 'RHO', 'MCJoin', 'PaMeCo']
    MAX_QUERIES = 17
    DATASETS = ['A', 'D']

    if config['experiment']:
        commons.remove_file(csv_file)
        commons.init_file(csv_file, "alg,dataset,queries,throughput\n")
        commons.compile_app('sgx', config='Enclave/Enclave3GB.config.xml',
                            application='MULTI_QUERY')

        for alg in algs:
            print("Run " + alg)

            for ds in DATASETS:
                for q in range(1, MAX_QUERIES+1):
                    run_join(alg, q, ds, config['threads'][alg], config['reps'])

    # commons.plot_concurrent_queries()
