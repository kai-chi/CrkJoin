#!/usr/bin/python3

import subprocess
import re
import statistics
from re import sub

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

import commons
import yaml

csv_file = "data/tpch-queries.csv"
png_file_base = "img/tpch-queries"


def run_join(alg, query, scale, threads, reps):
    print('Run query ' + str(query) + ' scale ' + str(scale) + ' with ' + str(alg) + ' ' + str(reps) + ' times')
    f = open(csv_file, "a")
    throughputs = []
    totalTimes = []
    selectionTimes = []
    joinTimes = []
    join1Times = []
    join2Times = []
    join3Times = []
    for i in range(reps):
        s = './app -a ' + str(alg) + ' -q ' + str(query) + ' -n ' + str(threads) + ' -s ' + str(scale) + ' -m 275 '
        stdout = ''
        try:
            stdout = subprocess.check_output(s, cwd="../", shell=True, stderr=subprocess.STDOUT) \
                .decode('utf-8')
        except subprocess.CalledProcessError as e:
            print ("App error:\n", e.output)
            print (e.stdout)
            exit()
        print(stdout)
        for line in stdout.splitlines():
            if "QueryThroughput" in line:
                throughput = re.findall("\d+\.\d+", line)[1]
                throughputs.append(float(throughput))
            elif "QueryTimeTotal" in line:
                totalTime = int(commons.escape_ansi(line.split(": ", 1)[1]))
                totalTimes.append(totalTime)
            elif "QueryTimeSelection" in line:
                selectionTime = int(re.findall(r'\d+', commons.escape_ansi(line))[2])
                selectionTimes.append(selectionTime)
            elif "QueryTimeJoin" in line:
                joinTime = int(re.findall(r'\d+', commons.escape_ansi(line))[2])
                joinTimes.append(joinTime)
            elif "Join 1 timer" in line:
                j1Time = int(commons.escape_ansi(line.split(": ", 1)[1]))
                join1Times.append(j1Time)
            elif "Join 2 timer" in line:
                j2Time = int(commons.escape_ansi(line.split(": ", 1)[1]))
                join2Times.append(j2Time)
            elif "Join 3 timer" in line:
                j3Time = int(commons.escape_ansi(line.split(": ", 1)[1]))
                join3Times.append(j3Time)

    avgThroughput = statistics.median(throughputs)
    avgTotalTime  = statistics.median(totalTimes)
    avgSelectionTime = statistics.median(selectionTimes)
    avgJoinTime = statistics.median(joinTimes)
    avgJ1 = statistics.median(join1Times)
    if not join2Times:
        avgJ2 = 0
    else:
        avgJ2 = statistics.median(join2Times)
    if not join3Times:
        avgJ3 = 0
    else:
        avgJ3 = statistics.median(join3Times)

    s = (alg + ',' + str(query) + ',' + str(scale) + ',' + str(round(avgTotalTime,2)) + ',' +
         str(round(avgSelectionTime,2)) + ',' + str(round(avgJoinTime,2)) + ',' + str(round(avgThroughput, 4)) +
         ',' + str(round(avgJ1,2)) + ',' + str(round(avgJ2,2)) + ',' + str(round(avgJ3,2)))
    print("AVG : " + s)
    f.write(s + "\n")
    f.close()


def plot():
    data = pd.read_csv(csv_file)
    scales = data['scale'].unique()
    data['timeJoin'] = data['timeJoinUs'].div(1000000)
    data['timeTotal'] = data['timeTotalUs'].div(1000000)
    for scale in scales:
        df = data[data['scale'] == scale]
        colors = list(map(lambda x:commons.color_alg(x), df['alg'].unique()))
        plt.figure(figsize=(3, 5))
        bgd_color = '#FFFFFF'
        sns.set_palette(sns.color_palette([bgd_color, bgd_color, bgd_color]))
        bx = sns.barplot(
            x='query',
            y='timeTotal',
            hue='alg',
            data=df,
            ci=None
        )

        sns.set_palette(sns.color_palette(colors))
        ax = sns.barplot(
            x='query',
            y='timeJoin',
            hue='alg',
            data=df,
            ci=None
        )

        linewidth = 0.5
        edgecolor = 'black'
        for patch in bx.patches:
            patch.set_linewidth(linewidth)
            patch.set_edgecolor(edgecolor)

        for patch in ax.patches:
            patch.set_linewidth(linewidth)
            patch.set_edgecolor(edgecolor)

        handles, labels = ax.get_legend_handles_labels()
        handles.reverse()
        labels.reverse()
        unique = [(h, l) for i, (h, l) in enumerate(zip(handles, labels)) if l not in labels[:i]]
        unique.reverse()
        ax.legend(*zip(*unique))

        plt.ylabel("Query Execution Time [s]")
        plt.xlabel('TPC-H Query')

        commons.savefig(png_file_base + '-scale-' + str(scale) + '.png')


def plot_shared():
    data = pd.read_csv(csv_file)
    data = data[(data['scale'] == 1) | (data['scale'] == 10)]
    scales = data['scale'].unique()
    data['timeJoin'] = data['timeJoinUs'].div(1000000)
    data['timeTotal'] = data['timeTotalUs'].div(1000000)
    data['timeJ1'] = data['join1Us'].div(1000000)
    data['timeJ2'] = data['join2Us'].div(1000000)
    data['timeJ3'] = data['join3Us'].div(1000000)

    data['timeJ2'] += data['timeJ1']
    data['timeJ3'] += data['timeJ2']
    fig = plt.figure(figsize=(9,2.5))
    plt.rcParams.update({'font.size': 15})
    i = 0
    alpha = 1
    for scale in scales:
        df = data[data['scale'] == scale]
        colors = list(map(lambda x:commons.color_alg(x), df['alg'].unique()))
        plt.subplot(1, len(scales), i+1)
        plt.gca().yaxis.grid(linestyle='dashed')
        plt.gca().set_axisbelow(True)
        bgd_color = '#FFFFFF'
        sns.set_palette(sns.color_palette([bgd_color, bgd_color, bgd_color, bgd_color]))
        bx = sns.barplot(
            x='query',
            y='timeTotal',
            hue='alg',
            data=df,
            ci=None
        )

        sns.set_palette(sns.color_palette(colors))
        linewidth = 0.8
        edgecolor = 'black'

        ax = sns.barplot(
            x='query',
            y='timeJ3',
            hue='alg',
            data=df,
            ci=None,
            alpha=alpha
        )
        for patch in ax.patches:
            patch.set_linewidth(linewidth)
            patch.set_edgecolor(edgecolor)

        ax = sns.barplot(
            x='query',
            y='timeJ2',
            hue='alg',
            data=df,
            ci=None,
            alpha=alpha
        )
        for patch in ax.patches:
            patch.set_linewidth(linewidth)
            patch.set_edgecolor(edgecolor)

        ax = sns.barplot(
            x='query',
            y='timeJ1',
            hue='alg',
            data=df,
            ci=None,
            alpha=alpha
        )
        for patch in ax.patches:
            patch.set_linewidth(linewidth)
            patch.set_edgecolor(edgecolor)

        for patch in bx.patches:
            patch.set_linewidth(linewidth)
            patch.set_edgecolor(edgecolor)

        if i == 0:
            plt.ylabel("Execution Time [s]")
        elif i == 1 or i == 2:
            plt.ylabel('')

        plt.xlabel('TPC-H Query', labelpad=-1)
        plt.title('(' + chr(97+i) + ") Scale Factor $\it{" + str(scale) + "}$", y=-0.39)
        ax.get_legend().remove()
        i = i + 1

    handles, labels = fig.axes[-1].get_legend_handles_labels()
    handles.reverse()
    labels.reverse()
    unique = [(h, l) for i, (h, l) in enumerate(zip(handles, labels)) if l not in labels[:i]]
    unique.reverse()
    fig.legend(*zip(*unique), fontsize='small', frameon=0,
               # ncol=4, bbox_to_anchor=(0.12, 0.85), loc='lower left',
               ncol=1, bbox_to_anchor=(0.9, 0.45), loc='lower left',
               borderaxespad=0, handletextpad=0.25, columnspacing=1.5)
    commons.savefig(png_file_base + '.png', tight_layout=False)


if __name__ == '__main__':

    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    algs = ['CrkJoin', 'RHO', 'MCJoin']
    queries = [3, 10, 12, 19]
    scales = [1, 10]

    if config['experiment']:
        commons.remove_file(csv_file)
        commons.init_file(csv_file, "alg,query,scale,timeTotalUs,timeSelectionUs,timeJoinUs,throughput,join1Us,join2Us,join3Us\n")
        commons.compile_app('sgx', config='Enclave/Enclave4GB.config.xml',
                            application='TPCH')
        for scale in scales:
            print("Scale " + str(scale))
            for alg in algs:
                print("Run " + alg)
                for query in queries:
                    run_join(alg, query, scale, config['threads'][alg], config['reps'])

    # plot()
    plot_shared()
