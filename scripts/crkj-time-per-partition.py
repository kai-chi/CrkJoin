#!/usr/bin/python3

import subprocess
import re
import matplotlib.pyplot as plt
import csv

import numpy as np
import pandas as pd
import yaml

import commons

ucsv = 'data/crkj-time-per-partition-uniform.csv'
scsv = 'data/crkj-time-per-partition-skew.csv'
png_file = 'img/crkj-time-per-partition'
avg_throughput = 0


def run_join_uniform(bits, reps, threads=2):
    f = open(ucsv, 'a')
    s = commons.PROG + ' -a CRKJ ' + ' -d A ' \
        + ' -b ' + str(bits) + ' -n ' + str(threads)
    for i in range(reps):
        stdout = subprocess.check_output(s, cwd="../",shell=True).decode('utf-8')
        # print(stdout)
        for line in stdout.splitlines():
            if 'TimerPerPartition' in line:
                title, data = line.split(":")
                data.strip(" ")
                data = commons.escape_ansi(data)
                f.write(data + '\n')
    f.close()


def run_join_skew(bits, reps, threads, skew):
    f = open(scsv, 'a')
    s = commons.PROG + ' -a CRKJ ' + ' -d A ' \
        + ' -b ' + str(bits) + ' -z ' + str(skew) + ' -n ' + str(threads)
    for i in range(reps):
        stdout = subprocess.check_output(s, cwd="../",shell=True).decode('utf-8')
        # print(stdout)
        for line in stdout.splitlines():
            if 'TimerPerPartition' in line:
                title, data = line.split(":")
                data.strip(" ")
                data = commons.escape_ansi(data)
                f.write(data + '\n')
    f.close()


def plot(withSkew=False):
    plt.figure(figsize=(5,1))
    import csv
    with open(ucsv, newline='') as f:
        reader = csv.reader(f)
        data = list(reader)

    i = 0
    for d in data:
        values = [int(x) for x in d]
        plt.plot(values, marker='o', markersize=1.5, linewidth=1,
            label='thread-' + str(i))
        i = i + 1
    # plt.legend()
    plt.ylabel('Time per\npartition [ms]')
    plt.xlabel('Partition mask', labelpad=-8)
    plt.xticks([0x00,0xff])
    plt.xlim([-1,255])
    plt.gca().set_xticklabels(['0x00','0xFF'])
    # ax.set_xticks(x_ticks)
    # ax.set_xticklabels(data['alg'])
    commons.savefig(png_file + '.png', tight_layout=False)


if __name__ == '__main__':
    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    bits = 10
    threads = 4

    if config['experiment']:
        commons.remove_file(ucsv)
        commons.remove_file(scsv)
        commons.init_file(ucsv,"")
        commons.init_file(scsv,"")

        commons.compile_app('sgx', application='TEE_BENCH', flags=['MEASURE_PARTITIONS'])
        # run_join_uniform(bits, config['reps'], threads)
        run_join_skew(bits, config['reps'], threads, skew=0.4)

    plot()
