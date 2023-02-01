#!/usr/bin/python3

import subprocess
import re
import matplotlib.gridspec as gridspec
import matplotlib.pyplot as plt
import numpy as np
import csv
import commons
import statistics
import yaml
from matplotlib.patches import Patch


csv_file = "data/crkj-phases.csv"
png_file = "img/crkj-phases.png"

colors = {"build":"g", "partition":"deeppink", "probe":"dodgerblue"}


def run_join(mode, alg, dsName, sizeR, sizeS, threads, reps):
    f_phases = open(csv_file, 'a')

    throughput = ''
    dic_phases = {}
    for i in range(reps):
        stdout = subprocess.check_output(commons.PROG + " -a " + alg + " -n " + str(threads)
                                         + ' -r ' + str(sizeR) + ' -s ' + str(sizeS)
                                         , cwd="../", shell=True) \
            .decode('utf-8')
        print(str(i+1) + '/' + str(reps) + ': ' +
              mode + "," + alg + "," + dsName + "," + str(sizeR))
        for line in stdout.splitlines():
            # find throughput for the first graph
            if "Throughput" in line:
                throughput = re.findall("\d+\.\d+", line)[1]
                # find phase for the second graph
            if "Phase" in line:
                words = line.split()
                phase_name = words[words.index("Phase") + 1]
                value = int(re.findall(r'\d+', line)[-2])
                print (phase_name + " = " + str(value))
                if phase_name in dic_phases:
                    dic_phases[phase_name].append(value)
                else:
                    dic_phases[phase_name] = [value]

        print('Throughput = ' + str(throughput) + ' M [rec/s]')

    for x in dic_phases:
        res = statistics.median(dic_phases[x])
        s = mode + "," + alg + "," + dsName + ',' + str(sizeR) + ',' + str(sizeS) \
            + "," + x + "," + str(res)
        print(s)
        print(dic_phases[x])
        f_phases.write(s + '\n')

    f_phases.close()


def plot():
    csvf = open(csv_file, mode = 'r')
    csvr = csv.DictReader(csvf)
    all_data = list(csvr)
    all_data = list(filter(lambda x:x['phase'] != "Total", all_data))
    datasets = sorted(set(map(lambda x:x['ds'], all_data)))
    phases = sorted(set(map(lambda x:x['phase'], all_data)))
    to_datasets = [[y for y in all_data if y['ds'] == x] for x in datasets]
    fig = plt.figure(figsize=(8,8))
    outer = gridspec.GridSpec(2,2)
    for i in range(len(datasets)):
        ds = datasets[i]
        ax = plt.Subplot(fig, outer[i+2])
        bar = 0
        rs = sorted(set(map(lambda x:int(x['sizeR']), to_datasets[i])))
        to_rs = [[y for y in to_datasets[i] if int(y['sizeR']) == x] for x in rs]
        for r in range(len(rs)):
            agg = 0
            val_build = int(to_rs[r][0]['cycles'])
            val_part  = int(to_rs[r][1]['cycles'])
            val_probe = int(to_rs[r][2]['cycles'])
            total = val_build + val_part + val_probe
            ax.bar(bar, val_build / total, bottom = agg, color=colors['build'], edgecolor='black')
            agg += val_build / total
            ax.bar(bar, val_part / total, bottom = agg, color=colors['partition'], edgecolor='black')
            agg += val_part / total
            ax.bar(bar, val_probe / total, bottom = agg, color=colors['probe'], edgecolor='black')
            bar = bar + 1
        ax.set_xticks(np.arange(len(rs)))
        labels = [x / 1000000 for x in rs]
        ax.set_xticklabels(labels, rotation=45)
        if i == 0:
           ax.set_ylabel('CPU cycles per tuple normalized')
        ax.set_xlabel('|R| [M tuples]')
        ax.set_title('(' + chr(97+i) + ") Dataset $\it{" + ds + "}$", y=-0.4)
        fig.add_subplot(ax)

    for i in range(len(datasets)):
        ds = datasets[i]
        ax = plt.Subplot(fig, outer[i])
        agg = 0
        bar = 0
        rs = sorted(set(map(lambda x:int(x['sizeR']), to_datasets[i])))
        to_rs = [[y for y in to_datasets[i] if int(y['sizeR']) == x] for x in rs]
        for r in range(len(rs)):
            for p in range(len(to_rs[r])):
                tuples = int(to_rs[r][p]['sizeR']) + int(to_rs[r][p]['sizeS']) 
                val = float(to_rs[r][p]['cycles']) / tuples
                l = to_rs[r][p]['phase']
                # first print a white background to cover the grid lines
                ax.bar(bar, val, bottom=agg, label=l,
                       color='white', edgecolor='black')
                #now print the real bar
                ax.bar(bar, val, bottom=agg, label=l,
                       color=colors[l], edgecolor='black')
                agg = agg + val
            agg = 0
            bar = bar + 1
        ax.set_xticks(np.arange(len(rs)))
        labels = [x / 1000000 for x in rs]
        ax.set_xticklabels(labels, rotation=45)
        if i == 0:
           ax.set_ylabel('CPU cycles per tuple')
        ax.set_xlabel('|R| [M tuples]')
        ax.set_title('(' + chr(97+i) + ") Dataset $\it{" + ds + "}$", y=-0.4)
        fig.add_subplot(ax)

    leg = []
    for p in phases:
        leg.append(Patch(facecolor=colors[p], edgecolor='black',
                         label=p))
    fig.legend(handles=leg, ncol=5, frameon=False,
               loc="upper left", bbox_to_anchor=(0.005,1.04,1,0), fontsize=12,
               handletextpad=0.5)
    commons.savefig(png_file)


if __name__ == '__main__':
    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    threads = 1

    if config['experiment']:
        commons.remove_file(csv_file)
        commons.init_file(csv_file, "mode,alg,ds,sizeR,sizeS,phase,cycles\n")

        for mode in ['sgx']:
            commons.compile_app(mode)
            # dataset S=10R
            for alg in ['CrkJoin']:
                for r in [1,2,4,8,16,32,64,128,256]:
                    x = r * 1000000 # in millions
                    run_join(mode, alg, 'S=10R', x, 10*x, threads, config['reps'])
            # dataset S=R
            for alg in ['CrkJoin']:
                for r in [1,2,4,8,16,32,64,128,256,512,1024,2048]:
                    x = r * 1000000 # in millions
                    run_join(mode, alg, 'S=R', x, x, threads, config['reps'])


    plot()
