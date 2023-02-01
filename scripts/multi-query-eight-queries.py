#!/usr/bin/python3

import subprocess
import re
import statistics
import sys

import matplotlib.pyplot as plt

import pandas as pd
from matplotlib.patches import Patch

import commons

csv_file = "data/multi-query-eight-queries.csv"
png_file = "img/multi-query-eight-queries.png"


def run_join(alg, dataset, queries, threads, reps):
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
    s = (str(dataset) + ',' + str(alg) + ',' + str(round(res,4)))
    print("AVG : " + s)
    f.write(s + "\n")
    f.close()


def plot():
    data = pd.read_csv(csv_file)
    datasets = data['dataset'].unique()
    fig = plt.figure(figsize=(10,1.7))

    for i in range(len(datasets)):
        ax = plt.subplot(1,len(datasets), i+1)
        ds = datasets[i]
        d = data.loc[data['dataset'] == ds]
        colors = list(map(commons.color_alg, d['alg'].tolist()))
        d.plot.bar(ax=ax,x='alg',y='throughput',color=colors,
                   legend=False, edgecolor='black', width=0.8)
        ax.yaxis.grid(linestyle='dashed')
        ax.set_axisbelow(True)
        ax.set_xticks([])
        ax.set_yscale('log')
        ax.set_ylim([0.005, 30])
        ax.set_title('(' + chr(97+i) + ") Dataset $\it{" + ds + "}$", y=-0.2)
        ax.set_xlabel('')
        ax.set_yticks([0.01, 0.1, 1, 10])
        if i == 0:
            ax.set_ylabel('Throughput [M rec/s]')
            # ax.annotate('BHJ', xy=(0.8, 0.01),  xycoords='data',
            #             xytext=(0.3, 0.4), textcoords='axes fraction',
            #             arrowprops=dict(shrink=0.05, width=0.5,
            #                             headwidth=4, color='gray'),
            #             horizontalalignment='center',fontsize=9
            #             )
        else:
            # ax.set_yticks([0.01, 0.1, 1, 10])
            # ax.tick_params(left=False)
            ax.set(yticklabels=[])
        for w, v in enumerate(d['throughput']):
            if v <= 0.001:
                ax.text(w-0.3, 0.01, 'Timeout', rotation=90)
    plt.subplots_adjust(wspace=0.1)
    handles = [
        Patch(color=commons.color_alg('BHJ'), label='BHJ'),
        Patch(color=commons.color_alg('RHO'), label='RHO'),
        Patch(color=commons.color_alg('MCJoin'), label='MCJoin'),
        Patch(color=commons.color_alg('PaMeCo'), label='PaMeCo'),
        Patch(color=commons.color_alg('CrkJ'), label='CrkJ'),
    ]
    fig.legend(handles=handles, ncol=6, bbox_to_anchor=(0.85, 1.07),
               fontsize=12, handletextpad=0.4, columnspacing=1.8,
               frameon=False)
    commons.savefig(png_file, tight_layout=False)


if __name__ == '__main__':

    config = commons.read_config(sys.argv)

    algs = ['BHJ', 'RHO', 'MCJoin', 'PaMeCo', 'CRKJ']
    QUERIES = 8

    if config['experiment']:
        commons.compile_app('sgx', config='Enclave/Enclave8GB.config.xml',
                            application='MULTI_QUERY')
        commons.remove_file(csv_file)
        commons.init_file(csv_file, "dataset,alg,throughput\n")

        for ds in commons.get_crkj_dataset_names():
            for alg in algs:

                print("Run " + alg)
                run_join(alg, ds, QUERIES, config['threads'][alg], config['reps'])

    plot()
