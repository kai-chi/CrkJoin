#!/usr/bin/python3
import pandas as pd
import yaml

import commons
import re
import statistics
import subprocess
import csv
import matplotlib.pyplot as plt

res_file = "data/crkj-scale-s.csv"
mb_of_data = 131072


def run_join(alg, size_r, size_s, threads, reps, mode):
    f = open(res_file, "a")
    results = []
    for i in range(0,reps):
        stdout = subprocess.check_output(commons.PROG + " -a " + alg + " -r " + str(size_r) + " -s " + str(size_s) + " -n " + str(threads), cwd="../",shell=True).decode('utf-8')
        for line in stdout.splitlines():
            if "Throughput" in line:
                throughput = re.findall("\d+\.\d+", line)[1]
                s = (mode + "," + alg + "," + str(threads) + "," + str(round(size_r/mb_of_data,2)) + "," + str(round(size_s/mb_of_data,2)) + "," + str(throughput))
                results.append(float(throughput))
                print (s)
    res = statistics.median(results)
    s = (mode + "," + alg + "," + str(threads) + "," + str(size_r) + "," + str(size_s) + "," + str(round(res,2)))
    print ("AVG : " + s)
    f.write(s + "\n")
    f.close()


def plot():
    # csvf = open(res_file, mode='r')
    # csvr = csv.DictReader(csvf)
    # all_data = list(csvr)
    # r_sizes = sorted(set(map(lambda x:int(x['sizeR']), all_data)))
    # splitted = [[y for y in all_data if y['sizeR'] == str(x)] for x in r_sizes]
    # plt.figure()
    # for i in range(0, len(r_sizes)):
    #     s_sizes = sorted(set(map(lambda x:int(x['sizeS'])/r_sizes[i], splitted[i])))
    #     plt.plot(s_sizes, list(map(lambda x:float(x['throughput']), splitted[i])), label=str(r_sizes[i]/1000000)+'M',
    #              linewidth=2, marker='o')
    # # plt.xscale('log')
    # plt.xlabel('S cardinality')
    # plt.ylabel('Throughput [M rec/s]')
    # plt.gca().yaxis.grid(linestyle='dashed')
    # plt.legend()
    # commons.savefig('img/crkj-scale-s.png')

    all_data = pd.read_csv(res_file)
    max = all_data['throughput'].max()
    fig = plt.figure(figsize=(3,3))
    i = 0
    ax = plt.gca()
    for key, grp in all_data.groupby('sizeR'):
        grp.plot(ax=ax, x='ratioS',y='throughput',
                 label=str(int(key)/1000000), marker='o',
                 color=commons.color_categorical(i), legend=False)
        i = i + 1

    plt.ylim([0, 1.1*max])
    plt.xlabel('|S|:|R| ratio')
    plt.ylabel("Throughput [M rec / sec]")
    # plt.xticks([1,2,3,4,5,6,7,8,9,10])
    ax.yaxis.grid(linestyle='dashed')
    lines, labels = fig.axes[-1].get_legend_handles_labels()
    labels = [str(x) + ' M' for x in labels]
    fig.legend(lines, labels, fontsize=10, frameon=0,
               ncol=3, bbox_to_anchor=(0.1, 0.92), loc='lower left',
               borderaxespad=0, handletextpad=1, columnspacing=1,
               markerscale=1)
    commons.savefig('img/crkj-scale-s.png')


if __name__ == '__main__':
    timer = commons.start_timer()
    r_sizes = [4000000,
               8000000,
               16000000,
               32000000,
               64000000,
               128000000]

    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    reps = config.get('reps', 1)
    threads = config.get('threads', {}).get('CRKJ', 1)

    if config['experiment']:
        commons.compile_app('sgx')
        commons.remove_file(res_file)
        commons.init_file(res_file, "mode,alg,threads,sizeR,sizeS,throughput\n")
        for r_size in r_sizes:
            for i in range(1, 10):
                s_size = r_size * i
                run_join('CRKJ', r_size, s_size, threads, reps, 'sgx')

    plot()
    commons.stop_timer(timer)
