#!/usr/bin/python3

import subprocess
import re
import statistics
import matplotlib as mpl
import matplotlib.pyplot as plt
import csv

import pandas as pd
from matplotlib.lines import Line2D

import commons
import yaml

fname_output = "data/crkj-multi-threading.csv"
plot_fname_base = "img/crkj-multi-threading"


def run_join(mode, prog, alg, dataset, threads, reps, memory):
    f = open(fname_output, "a")
    results = []

    for i in range(0,reps):
        s = str(prog) + " -a " + str(alg) + \
            " -d " + str(dataset) + " -n " + str(threads) + \
            ' -m ' + str(memory)
        if 'dynamic' in mode:
            s += ' -b -1 '
        try:
            stdout = subprocess.check_output(s, cwd="../",shell=True)\
                .decode('utf-8')
        except subprocess.CalledProcessError as e:
            raise RuntimeError("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))
        print(stdout)
        for line in stdout.splitlines():
            if "Throughput" in line:
                throughput = re.findall("\d+\.\d+", line)[1]
                s = (alg + " " + str(dataset) + " " + str(threads) + " " + str(throughput))
                results.append(float(throughput))
                print(s)

    res = statistics.median(results)
    if 'static' in mode:
        alg += '_static'

    s = (str(dataset) + "," + alg + "," + str(threads) + "," + str(round(res,4)))
    print("AVG : " + s)
    f.write(s + "\n")
    f.close()


def plot_threading():
    mcjoin_throughput = {
        'A': 19.53,
        'B': 6.70,
        'C': 4.61,
        'D': 10.83
    }
    ms = 5
    all_data = pd.read_csv(fname_output)
    algos = sorted(all_data['alg'].unique())
    # datasets = sorted(set(map(lambda x:x['dataset'], all_data)))
    datasets = sorted(all_data['dataset'].unique())
    max_throughput = all_data['throughput'].max()
    fig = plt.figure(figsize=(2.5*len(datasets),1.6))

    i = 0
    for dsKey, dsGrp in all_data.groupby('dataset'):
        plt.subplot(1, len(datasets), i + 1)
        ax = plt.gca()
        for algKey, algGrp in dsGrp.groupby('alg'):
            algGrp.plot(ax=ax, x='threads', y='throughput',
                        label=algKey, marker=commons.marker_alg(algKey),
                        markersize=ms, markeredgecolor='black',
                        markeredgewidth=0.5, legend=False,
                        linewidth=2, color=commons.color_alg(algKey))
        commons.draw_horizontal_lines(plt, [mcjoin_throughput[dsKey]],
                                      color=commons.color_alg('MCJoin'),
                                      linewidth=2)
        # make the figure neat
        plt.ylim([0, 1.1*max_throughput])
        plt.xlabel('Number of threads', labelpad=-1.8)
        if i == 0:
            plt.ylabel("Throughput\n[M rec / sec]")
        else:
            ax.set(yticklabels=[])
        # plt.xticks([2,4,6,8])
        # plt.yticks([0,10,20,30,40])
        ax.yaxis.grid(linestyle='dashed')
        plt.yticks([25,50,75, 100])
        plt.subplots_adjust(wspace=0.1)
        plt.title('(' + chr(97+i) + ") Dataset $\it{" + dsKey + "}$", y=-0.44)
        i = i + 1

    lines, labels = fig.axes[-1].get_legend_handles_labels()
    labels.append('MCJoin')
    lines.append(Line2D([0],[0],color=commons.color_alg('MCJoin'),
                        linewidth=2, linestyle='--'))
    labels, lines = zip(*sorted(zip(labels, lines), key=lambda t: t[0]))
    for line in lines:
        line.set_linewidth(2.5)
    fig.legend(lines, labels, fontsize=12, frameon=0,
               ncol=8, bbox_to_anchor=(0.28, 0.845), loc='lower left',
               borderaxespad=0, handletextpad=1, columnspacing=1,
               markerscale=1.8)

    # save fig
    commons.savefig(plot_fname_base + ".png", tight_layout=False)


if __name__ == '__main__':
    timer = commons.start_timer()

    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    threads = 16
    modes = ['sgx']# ['sgx_dynamic', 'sgx_static']
    algs = ['CrkJoin', 'RHO']
    reps = config.get('reps', 1)

    if config['experiment']:
        enclave_string = 'Enclave/Enclave8GB.config.xml'
        commons.compile_app('sgx', config=enclave_string)
        commons.remove_file(fname_output)
        commons.init_file(fname_output, "mode,alg,dataset,threads,throughput\n")

        for alg in algs:
            memory = config.get('memory', {}).get(alg, 0)
            for mode in modes:
                for ds in commons.get_crkj_dataset_names():
                    for i in range(threads):
                        if 'CRKJ' in alg:
                            n = i + 1
                            if (n & (n-1) == 0) and n != 0:
                                run_join(mode, commons.PROG, alg, ds, i+1, reps, memory)
                        else:
                            run_join(mode, commons.PROG, alg, ds, i+1, reps, memory)

    plot_threading()

    commons.stop_timer(timer)