import csv
import errno
import getopt
import os
import re
import subprocess
import sys
from timeit import default_timer as timer
from datetime import timedelta
import matplotlib.pyplot as plt
from collections import namedtuple

import pandas as pd
import yaml

PROG = ' ./app '


def get_all_algorithms():
    return ["CHT", "INL", "PHT", "PSM", "RHO", "RHT", "RSM"]


def get_all_algorithms_extended():
    return ["CHT", 'INL' ,"MWAY", "PHT", "PSM", "RHT", "RHO", "RSM"]


def get_test_dataset_names():
    return ["cache-fit", "cache-exceed"]

def get_crkj_dataset_names():
    return ['A', 'B', 'C', 'D']


def get_join_dataset_names():
    return ['A', 'B']


def start_timer():
    return timer()


def stop_timer(start):
    print("Execution time: " + str(timedelta(seconds=(timer()-start))))


def escape_ansi(line):
    ansi_escape = re.compile(r'(\x9B|\x1B\[)[0-?]*[ -\/]*[@-~]')
    return ansi_escape.sub('', line)


def compile_app(mode: str, flags=None, config=None, debug=False, application='TEE_BENCH'):
    if flags is None:
        flags = []
    print("Make clean")
    subprocess.check_output(["make", "clean"], cwd="../")
    if "native" in mode:
        exec_mode = "native"
    # cover all sgx modes like sgx-affinity
    elif "sgx" in mode:
        exec_mode = "sgx"
    else:
        raise ValueError("Mode not found: " + mode)

    cflags = 'CFLAGS='
    if mode == 'native':
        cflags += ' -DNATIVE_COMPILATION '

    if mode == 'native-materialize':
        cflags += ' -DNATIVE_COMPILATION -DJOIN_MATERIALIZE '

    if mode == 'sgx-materialize' or mode == 'sgx-seal' or mode =='sgx-chunk-buffer':
        cflags += ' -DJOIN_MATERIALIZE '

    if mode == 'sgx-affinity':
        cflags += ' -DTHREAD_AFFINITY '

    for flag in flags:
        cflags += ' -D' + flag + ' '

    if config is None:
        enclave_string = ' CONFIG=Enclave/Enclave.config.xml '
    else:
        enclave_string = ' CONFIG=' + str(config) + ' '

    if str(application) not in ['TEE_BENCH', 'MULTI_QUERY', 'TPCH']:
        raise ValueError('Application not found: ' + str(application))

    app_flag = 'APP=' + str(application)

    print("Make " + mode + " enclave " + enclave_string + " with flags " + cflags)
    if debug:
        subprocess.check_output(["make", '-B' , exec_mode, 'SGX_DEBUG=1', 'SGX_PRERELEASE=0',
                                      enclave_string, app_flag, cflags], cwd="../", stderr=subprocess.DEVNULL)
    else:
        subprocess.check_output(["make", "-B", exec_mode, enclave_string, app_flag, cflags], cwd="../", stderr=subprocess.DEVNULL)


def make_app_radix_bits(sgx: bool, perf_counters: bool, num_passes: int, num_radix_bits: int):
    print('Make clean')
    subprocess.check_output(['make', 'clean'], cwd='../')
    flags = 'CFLAGS='
    prog = 'sgx' if sgx else 'native'

    if not sgx:
        flags += ' -DNATIVE_COMPILATION '
    if perf_counters:
        flags += ' -DPCM_COUNT '
    flags += ' -DNUM_PASSES=' + str(num_passes) + " "
    flags += ' -DNUM_RADIX_BITS=' + str(num_radix_bits)

    print(prog + ' ' + flags)
    subprocess.check_output(['make', '-B', prog, flags], cwd='../')


def make_app(sgx: bool, perf_counters: bool):
    print("Make clean")
    subprocess.check_output(["make", "clean"], cwd="../")
    if sgx:
        print("Make SGX app. perf_counters=" + str(perf_counters))
        if perf_counters:
            subprocess.check_output(["make", "-B", "sgx", "CFLAGS=-DPCM_COUNT"], cwd="../")
        else:
            subprocess.check_output(["make", "-B", "sgx"], cwd="../")
    else:
        print("Make native app. perf_counters=" + str(perf_counters))
        if perf_counters:
            subprocess.check_output(["make", "-B", "native", "CFLAGS=-DNATIVE_COMPILATION -DPCM_COUNT"], cwd="../")
        else:
            subprocess.check_output(["make", "-B", "native", "CFLAGS=-DNATIVE_COMPILATION"], cwd="../")


def remove_file(fname):
    try:
        os.remove(fname)
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise


def init_file(fname, first_line):
    f = open(fname, "x")
    print(first_line)
    f.write(first_line)
    f.close()

##############
# colors:
#
# 30s
# mint: bcd9be
# yellow brock road: e5cf3c
# poppy field: b33122
# powder blue: 9fb0c7
# egyptian blue: 455d95
# jade: 51725b
#
# 20s
# tuscan sun: e3b22b
# antique gold: a39244
# champagne: decd9d
# cadmium red: ad2726
# ultramarine: 393c99
# deco silver: a5a99f
#
# 40s
# war bond blue: 30356b
# keep calm & canary on: e9ce33
# rosie's nail polish: b90d08
# air force blue: 6d9eba
# nomandy sand: d1bc90
# cadet khaki: a57f6a
#
# 80s
# acid wash: 3b6bc6
# purple rain: c77dd4
# miami: fd69a5
# pacman: f9e840
# tron turqouise: 28e7e1
# powersuit: fd2455
#
# 2010s
# millennial pink: eeb5be
# polished copper: c66d51
# quartz: dcdcdc
# one million likes: fc3e70
# succulent: 39d36e
# social bubble: 0084f9
#############


def color_categorical(i):
    colors = {
        0:'#0fb5ae',
        1:'#4046ca',
        2:'#f68511',
        3:'#de3d82',
        4:'#7e84fa',
        5:'#72e06a',
        6:'#147af3',
        7:'#7326d3',
        8:'#e8c600',
        9:'#cb5d00',
        10:'#008f5d',
        11:'#bce931',
    }
    return colors[i]


def color_alg(alg):
    # colors = {"CHT":"#e068a4", "PHT":"#829e58", "RHO":"#5ba0d0"}
    colors = {"CHT":"#b44b20",  # burnt sienna
              "PHT":"#7b8b3d",  # avocado
              "PSM":"#c7b186",  # natural
              "BHJ":"#c7b186",
              "RHT":"#885a20",  # teak
              "RHO":"#e6ab48",  # harvest gold
              "RSM":"#4c748a",  # blue mustang
              'TinyDB': '#0084f9',  # avocado
              "OJ":"#fd2455",
              "OPAQ":"#c77dd4",
              "OBLI":"#3b6bc6",
              "INL":'#7620b4',
              'NL':'#20b438',
              'MWAY':'#fd2455',
              # 'PaMeCo':'#28e7e1',
              'PaMeCo':'#bcd9be',
              'CRKJ':"#b3346c", # moody mauve
              "CRKJS":"#deeppink",
              'CrkJ':"#b3346c", # moody mauve
              'CrkJoin':"#b3346c", # moody mauve
              'CrkJoin+Skew':"#7620b4", # moody mauve
              'GHJ':'black',
              'RHO_atomic':'deeppink',
              # 'MCJoin':'#0084f9', # social bubble
              'MCJoin':'#51725b', # social bubble
              'CRKJ_CHT':'deeppink',
              'oldCRKJ':"#7b8b3d", # avocado
              'CRKJ_static':"#4c748a", # blue mustang

              'RHO-sgx': '#e6ab48',
              'RHO-sgx-affinity':'g',
              'RHO-lockless':'deeppink',
              'RHO_atomic-sgx':'deeppink'}
    # colors = {"CHT":"g", "PHT":"deeppink", "RHO":"dodgerblue"}
    return colors[alg]


def marker_alg(alg):
    markers = {
        "CHT": 'o',
        "PHT": 'v',
        "PSM": 'P',
        "RHT": 's',
        "RHO": 'X',
        "RSM": 'D',
        "INL": '^',
        'MWAY': '*',
        'RHO_atomic': 'P',
        'CRKJ':'>',
        'CrkJoin':'>',
        'CrkJoin+Skew':'D',
        'CRKJoin':'>',
        'CRKJS':'*',
        'CRKJ_CHT':'^',
        'oldCRKJ':'X',
        'CRKJ_static': 'D',
        "MCJoin": 'o',
        'PaMeCo': 'P',

        'RHO-sgx': 'X',
        'RHO-lockless':'h',
        'RHO_atomic-sgx':'h'
    }
    return markers[alg]


def color_size(i):
    # colors = ["g", "deeppink", "dodgerblue", "orange"]
    colors = ["#e068a4", "#829e58", "#5ba0d0", "#91319f"]
    return colors[i]


def savefig(filename, font_size=15, tight_layout=True):
    plt.rcParams.update({'font.size': font_size})
    if tight_layout:
        plt.tight_layout()
    plt.savefig(filename, transparent = False, bbox_inches = 'tight', pad_inches = 0.1, dpi=200)
    print("Saved image file: " + filename)


def threads(alg, dataset):
    Entry = namedtuple("Entry", ["alg", "dataset"])
    d = {
        Entry("CHT", "A"): 7,
        Entry("PHT", "A"): 4,
        Entry("RHO", "A"): 2,
        Entry("CHT", "B"): 2,
        Entry("PHT", "B"): 1,
        Entry("RHO", "B"): 3,
    }
    if dataset == 'L':
        return d[Entry(alg, 'M')]

    return d[Entry(alg, dataset)]


def draw_vertical_lines(plt, x_values, linestyle='--', color='#209bb4', linewidth=1):
    for x in x_values:
        plt.axvline(x=x, linestyle=linestyle, color=color, linewidth=linewidth)


def draw_horizontal_lines(plt, y_values, linestyle='--', color='#209bb4', linewidth=1):
    for y in y_values:
        plt.axhline(y=y, linestyle=linestyle, color=color, linewidth=linewidth)


def read_config(input_argv, yaml_file='config.yaml'):
    with open(yaml_file, 'r') as file:
        config = yaml.safe_load(file)
    argv = input_argv[1:]
    try:
        opts, args = getopt.getopt(argv, 'r:',['reps='])
    except getopt.GetoptError:
        print('Unknown argument')
        sys.exit(1)
    for opt, arg in opts:
        if opt in ('-r', '--reps'):
            config['reps'] = int(arg)
    return config


def plot_concurrent_queries():
    queries_csv = 'data/multi-query-queries.csv'
    delays_csv = 'data/multi-query-delays.csv'
    png_file = 'img/concurrent-queries.png'

    pad_title = -35

    fig = plt.figure(figsize=(7,3))
    # plot queries
    plt.subplot(1, 2, 1)
    data = pd.read_csv(queries_csv)
    crkj_throughput = data.loc[data['alg'] == 'CRKJ']['throughput'].to_list()
    queries = data.loc[data['alg'] == 'CRKJ']['queries'].to_list()
    # improvement over RHO
    rho_throughput = data.loc[data['alg'] == 'RHO']['throughput'].to_list()
    l = min(len(crkj_throughput), len(rho_throughput))
    improvement = [a/b for a,b in zip(crkj_throughput[:l], rho_throughput[:l])]
    plt.plot(queries[:l], improvement, label='over RHO',
             marker='o', color=color_categorical(0))

    # improvement over RHO
    mcjoin_throughput = data.loc[data['alg'] == 'MCJoin']['throughput'].to_list()
    l = min(len(crkj_throughput), len(mcjoin_throughput))
    improvement = [a/b for a,b in zip(crkj_throughput[:l], mcjoin_throughput[:l])]
    plt.plot(queries[:l], improvement, label='over MCJoin',
             marker='v', color=color_categorical(3))

    plt.ylabel("CrkJ throughput improvement")
    plt.xlabel('Number of concurrent queries')
    # plt.title('a) CrkJ improvement for concurrent queries', y=-0.5, tex)
    plt.gca().set_title('a) CrkJ improvement for concurrent queries.',
                        y=0, pad=pad_title, verticalalignment="top", fontsize='medium')
    plt.yscale('log')
    plt.gca().yaxis.grid(linestyle='dashed')
    lines, labels = fig.axes[-1].get_legend_handles_labels()
    fig.legend(lines, labels, fontsize='small', frameon=0,
               ncol=3, bbox_to_anchor=(0.12, 0.92), loc='lower left',
               borderaxespad=0, handletextpad=0.25, columnspacing=1.5)

    # plot delays
    plt.subplot(1, 2, 2)
    markers = {
        5: 'o',
        10: 'v',
        15: 's'
    }
    csvf = open(delays_csv, mode='r')
    csvr = csv.DictReader(csvf)
    all_data = list(csvr)
    algos = sorted(set(map(lambda x: x['alg'], all_data)))
    to_algos = [[y for y in all_data if y['alg'] == x] for x in algos]
    max_throughput = max(set(map(lambda x: float(x['throughput']), all_data)))

    # plot single query performance
    plt.axhline(y=80.86, color=color_alg('CRKJ'), linestyle='--')
    plt.axhline(y=25.97, color=color_alg('RHO'), linestyle='--')
    for i in range(len(algos)):
        alg = algos[i]
        queries = sorted(set(map(lambda x: int(x['queries']), to_algos[i])))
        to_queries = [[y for y in to_algos[i] if int(y['queries']) == x] for x in queries]
        for q in range(len(queries)):
            delays = (list(map(lambda x: int(x['delay']), to_queries[q])))
            throughputs = list(map(lambda x: float(x['throughput']), to_queries[q]))
            plt.plot(delays, throughputs, label=alg+'-'+str(queries[q]), color=color_alg(alg),
                     linewidth=1, marker=markers[queries[q]], markeredgecolor='black',
                     markersize=4, markeredgewidth=0.5)
    # make the figure neat
    plt.ylabel("Avg query throughput [M rec / sec]")
    plt.xlabel('Delay between queries [ms]')
    plt.ylim([0, 1.1 * max_throughput])
    plt.gca().yaxis.grid(linestyle='dashed')
    plt.gca().set_axisbelow(True)
    plt.gca().set_title('b) Concurrent queries with delay.',
                        y=0, pad=pad_title, verticalalignment="top", fontsize='medium')
    lines, labels = fig.axes[-1].get_legend_handles_labels()
    fig.legend(lines, labels, fontsize='small', frameon=0,
               ncol=3, bbox_to_anchor=(0.6, 0.92), loc='lower left',
               borderaxespad=0, handletextpad=0.25, columnspacing=1.5)
    savefig(png_file)
