#!/usr/bin/python3

import subprocess
import re
import matplotlib.pyplot as plt
import csv

import yaml

import commons

csv_file = 'data/crkj-partition-phases.csv'
png_file = 'img/crkj-partition-phases'
avg_throughput = 0


class Partition:
    def __init__(self, id, timeMillis, cyclesHT, cyclesPartition, cyclesProbe):
        self.id = id
        self.timeMillis = timeMillis
        self.cyclesHT = cyclesHT
        self.cyclesPartition = cyclesPartition
        self.cyclesProbe = cyclesProbe


def run_join(sizeR, sizeS, bits, threads, reps):
    f = open(csv_file, 'a')
    s = commons.PROG + ' -a CrkJoin ' + ' -r ' + str(sizeR) + ' -s ' + str(sizeS) \
        + ' -n ' + str(threads) + ' -b ' + str(bits)
    partitions = []
    throughput = []
    stdout = subprocess.check_output(s, cwd="../",shell=True).decode('utf-8')
    print(stdout)
    for line in stdout.splitlines():
        if 'Throughput' in line:
            throughput.append(float(re.findall("\d+\.\d+", line)[1]))
        if 'Partition stats' in line:
            tmp = line.split(':', 1)
            tmp = tmp[1].split(' ')
            stats = tmp[1].split(',')
            assert len(stats) == 5
            f.write(tmp[1] + '\n')
            p = Partition(int(stats[0]), int(stats[1]), int(stats[2]), int(stats[3]),
                          int(stats[4]))
            partitions.append(p)
            # for t in range(len(times)):
            #     if i == 0:
            #         results.append(int(times[t]))
            #     else:
            #         results[t] += int(times[t])
    # results = [int(x / reps) for x in results]
    # global avg_throughput
    # avg_throughput = statistics.median(throughput)
    # print('Average throughput = ' + str(avg_throughput))
    f.close()


def plot(sizeR, sizeS):
    csvf = open(csv_file, mode = 'r')
    csvr = csv.DictReader(csvf)
    all_data = list(csvr)
    times = list(map(lambda x:int(x['time']), all_data))
    fig = plt.figure(figsize=(4,4))
    plt.plot(times, '-o', markersize=2)
    plt.ylabel('Time to process partition [ms]')
    plt.xlabel('Partition number')
    plt.text(80,0.8*max(times),'|R|=' + str(int(sizeR)/1000000) +
             'M\n' + '|S|=' + str(int(sizeS)/1000000) + 'M' +
             '\nThroughput=' + str(round(avg_throughput,2)) + ' [M rec/s]')
    commons.savefig(png_file + '-times.png')


def plot_phases(sizeR, sizeS):
    csvf = open(csv_file, mode='r')
    csvr = csv.DictReader(csvf)
    all_data = list(csvr)
    times = list(map(lambda x : int(x['timeMillis']), all_data))
    plt.figure(figsize=(4,4))
    plt.plot(times, '-o', markersize=2)
    plt.ylabel('Time to process partition [ms]')
    plt.xlabel('Partition number')
    plt.text(0.5*len(times),0.8*max(times),
             '|R|=' + str(int(sizeR)/1000000) + 'M\n' +
             '|S|=' + str(int(sizeS)/1000000) + 'M')
    commons.savefig(png_file + '-times.png')

    fig= plt.figure(figsize=(16,8))
    ax = plt.subplot(2, 1, 1)
    totalHT = sum(map(lambda x : int(x['cyclesHT']), all_data))
    totalPartition = sum(map(lambda x : int(x['cyclesPartition']), all_data))
    totalProbe = sum(map(lambda x : int(x['cyclesProbe']), all_data))
    totalTotal = totalHT + totalPartition + totalProbe
    for i in range(len(all_data)):
        agg = 0
        cyclesHT = int(all_data[i]['cyclesHT'])
        cyclesPartition = int(all_data[i]['cyclesPartition'])
        cyclesProbe = int(all_data[i]['cyclesProbe'])
        total = 1
        ax.bar(i, cyclesHT / total, bottom=agg, color='white', edgecolor='black')
        ax.bar(i, cyclesHT / total, bottom=agg, color='#77dd77', edgecolor='black')
        agg += cyclesHT / total
        ax.bar(i, cyclesPartition / total, bottom=agg, color='white', edgecolor='black')
        ax.bar(i, cyclesPartition / total, bottom=agg, color='deeppink', edgecolor='black')
        agg += cyclesPartition / total
        ax.bar(i, cyclesProbe / total, bottom=agg, color='white', edgecolor='black')
        ax.bar(i, cyclesProbe / total, bottom=agg, color='dodgerblue', edgecolor='black')
    ax.set_xlabel("Partition")
    ax.set_ylabel("Cycles per phase")
    ax.set_xlim(-1)
    plt.text(0.25*len(times),0.8*int(all_data[0]['cyclesPartition']),
             'buildHT   = ' + str(round(totalHT*100/totalTotal,2)) + ' %\n' +
             'partition = ' + str(round(totalPartition*100/totalTotal,2)) + ' %\n' +
             'probe     = ' + str(round(totalProbe*100/totalTotal,2)) + ' %'
             )
    ax = plt.subplot(2, 1, 2)
    for i in range(len(all_data)):
        agg = 0
        cyclesHT = int(all_data[i]['cyclesHT'])
        cyclesPartition = int(all_data[i]['cyclesPartition'])
        cyclesProbe = int(all_data[i]['cyclesProbe'])
        total = cyclesHT + cyclesPartition + cyclesProbe
        if total == 0:
            total = 1
        ax.bar(i, cyclesHT / total, bottom=agg, color='white', edgecolor='black')
        ax.bar(i, cyclesHT / total, bottom=agg, color='#77dd77', edgecolor='black')
        agg += cyclesHT / total
        ax.bar(i, cyclesPartition / total, bottom=agg, color='white', edgecolor='black')
        ax.bar(i, cyclesPartition / total, bottom=agg, color='deeppink', edgecolor='black')
        agg += cyclesPartition / total
        ax.bar(i, cyclesProbe / total, bottom=agg, color='white', edgecolor='black')
        ax.bar(i, cyclesProbe / total, bottom=agg, color='dodgerblue', edgecolor='black')
    ax.set_xlabel("Partition")
    ax.set_ylabel("Cycles per phase")
    ax.set_xlim(-1)
    commons.savefig(png_file + '-cycles.png')


if __name__ == '__main__':
    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    threads = 1
    reps = 1
    bits = 8
    sizeR = 128000000
    sizeS = 1280000000

    if config['experiment']:
        commons.remove_file(csv_file)
        commons.init_file(csv_file,"id,timeMillis,cyclesHT,cyclesPartition,cyclesProbe\n")

        commons.compile_app('sgx')
        run_join(sizeR, sizeS, bits, threads, reps)

    plot_phases(sizeR, sizeS)
