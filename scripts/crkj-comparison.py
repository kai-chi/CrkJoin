#!/usr/bin/python3
import math
import subprocess
import re
import statistics
import matplotlib.pyplot as plt
import csv

from matplotlib.patches import Patch

import commons
import yaml

csv_file = "data/crkj-comparison.csv"
png_file = "img/crkj-comparison.png"


def run_join(alg, ds, threads, reps):
    f = open(csv_file, "a")
    results = []
    memRestriction = 275

    for i in range(0,reps):
        s = './app -a ' + str(alg) + ' -n ' + str(threads) + ' -d ' + str(ds) + ' -m ' + str(memRestriction)
        stdout = subprocess.check_output(s, cwd="../",shell=True)\
            .decode('utf-8')
        print(stdout)
        for line in stdout.splitlines():
            if "Throughput" in line:
                throughput = re.findall("\d+\.\d+", line)[1]
                results.append(float(throughput))

    res = statistics.median(results)
    s = (alg + ',' + str(threads) + ',' + str(ds) + ',' + str(memRestriction) + ',' + str(round(res,2)))
    print ("AVG : " + s)
    f.write(s + "\n")
    f.close()


def getWidth(alg):
    array = ['MCJoin', 'PaMeCo']
    if alg in array:
        return 0.4
    else:
        return 0.8


def plot_1row():
    csvf = open(csv_file, mode='r')
    csvr = csv.DictReader(csvf)
    all_data = list(csvr)
    fig = plt.figure(figsize=(10,1.7))
    algos = list(map(lambda x : x['alg'], all_data))
    datasets = sorted(set(map(lambda x:x['ds'], all_data)))
    to_datasets = [[y for y in all_data if y['ds'] == x] for x in datasets]
    # gs = gridspec.GridSpec(1, len(datasets))
    # gs.update(hspace=10)

    for i in range(len(datasets)):
        ax = plt.subplot(1,len(datasets),i+1)
        # ax = plt.subplot(gs[i])
        ax.yaxis.grid(linestyle='dashed')
        ax.set_axisbelow(True)
        data = to_datasets[i]
        throughputs = list(map(lambda x: float(x['throughput']), data))
        colors = list(map(lambda x:commons.color_alg(x), algos))
        # widths = list(map(lambda x : getWidth(x['alg']), data))
        # x_pos = np.arange(len(algos))
        # x_pos = [0, 1, 2, 2.5, 3.5, 4, 5]
        # x_pos = [0, 0.8, 1.6, 2.0, 2.6, 3.0, 3.8]
        # x_pos = [0, 0.8, 1.8, 2.6, 3.0, 3.8]
        x_pos = [0, 1, 2, 3, 4, 5]
        plt.bar(x_pos, throughputs, color=colors, edgecolor='black')
        # make the figure neat
        # plt.xlabel('Join Algorithm')
        if i == 0:
            plt.ylabel("Throughput [M rec / sec]")
            ax.annotate('TinyDB', xy=(-0.1, 40),  xycoords='data',
                        xytext=(0.14, 0.7), textcoords='axes fraction',
                        arrowprops=dict(shrink=0.05, width=0.5,
                                        headwidth=4, color='gray'),
                        horizontalalignment='center',fontsize=9
                        )
            # ax.annotate('single-threaded', xy=(1.6, 17),  xycoords='data',
            #             xytext=(0.05, 0.90), textcoords='axes fraction',
            #             arrowprops=dict(shrink=0.05, width=0.5,
            #                             headwidth=4, color='gray'),
            #             ha='left',
            #             va='center',
            #             fontsize=9
            #             )
            # ax.annotate('multi-\nthreaded', xy=(2, 22),  xycoords='data',
            #             xytext=(0.58, 0.45), textcoords='axes fraction',
            #             arrowprops=dict(shrink=0.05, width=0.5,
            #                             headwidth=4, color='gray'),
            #             horizontalalignment='center',
            #             fontsize=9
            #             )
        # plt.tick_params(
        #     axis='x',          # changes apply to the x-axis
        #     which='major',      # both major and minor ticks are affected
        #     bottom=False,      # ticks along the bottom edge are off
        #     top=False)         # ticks along the top edge are off
        ticks = ['TinyDB', 'BHJ', 'st   mt\nRHO', 'st   mt\nMCJoin/\nPaMeCo', 'CrkJoin']
        # x_pos = [0, 1, 2.25, 3.75, 5]
        x_pos = [0, 0.8, 1.8, 2.8, 3.8]
        plt.xticks(x_pos, [], fontsize=8, rotation=90)
        plt.ylim(top=90)
        plt.yticks([25,50,75])
        if i != 0:
            ax.set(yticklabels=[])
        plt.title('(' + chr(97+i) + ") Dataset $\it{" + datasets[i] + "}$", y=-0.2)
        xlocs, xlabs = plt.xticks()
        for x, y in zip(xlocs, throughputs):
            if y == 0:
                plt.text(x-0.25, y+4, 'Timeout', rotation=90, size=9)
            elif y < 1:
                plt.text(x-0.25, y+4, str(y), rotation=90, size=12)

        ax.set_xticks([], minor=False)
        ax.minorticks_off()
        # ax.set_xticks(, minor=True)
        # ax.set_xticklabels(x_pos, ticks)

        # xticks = ax.xaxis.get_major_ticks()
        # xticks[2].tick1line.set_visible(False)
        # xticks[3].tick1line.set_visible(False)

    plt.subplots_adjust(wspace=0.1)
    handles = [
        Patch(color=commons.color_alg('TinyDB'), label='TinyDB'),
        Patch(color=commons.color_alg('BHJ'), label='BHJ'),
        Patch(color=commons.color_alg('RHO'), label='RHO'),
        Patch(color=commons.color_alg('MCJoin'), label='MCJoin'),
        Patch(color=commons.color_alg('PaMeCo'), label='PaMeCo'),
        Patch(color=commons.color_alg('CrkJoin'), label='CrkJoin'),
    ]
    fig.legend(handles=handles, ncol=6, bbox_to_anchor=(0.88, 1.08),
                       fontsize=12, handletextpad=0.3, columnspacing=1.8,
               frameon=False)
    # save fig
    commons.savefig(png_file, tight_layout=False)


if __name__ == '__main__':

    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    algs = ['NL', 'BHJ', 'RHO', 'MCJoin', 'PaMeCo', 'CrkJoin']
    datasets = ['A', 'B', 'C', 'D']

    if config['experiment']:
        commons.remove_file(csv_file)
        commons.init_file(csv_file, "alg,threads,ds,memRestriction,throughput\n")

        commons.compile_app('sgx', config='Enclave/Enclave4GB.config.xml')
        for ds in datasets:
            for alg in algs:
                print("Run " + alg)
                run_join(alg, ds, config['threads'], config['reps'])

    plot_1row()
