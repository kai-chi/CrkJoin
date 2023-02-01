#!/usr/bin/python3
import sys

import pandas as pd
from matplotlib import pyplot as plt
import seaborn as sns
from matplotlib.patches import Patch

from scripts import commons

csv_file = 'data/top-figure.csv'
csv_file_native = 'data/top-figure-native.csv'
png_file = 'img/top-figure.png'


def plot():
    data = pd.read_csv(csv_file)
    data_native = pd.read_csv(csv_file_native)

    colors = list(map(commons.color_alg, data['alg'].tolist()))
    fig = plt.figure(figsize=(5, 2.6))
    ax = plt.gca()
    width=1.3
    shift=0.3
    hatch='/'
    hc = '#363636'
    lw=1.7
    x = [0,2,4,6,8,10]
    ax.bar(x[:5], data_native['throughput'], edgecolor=colors, color='white', width=width,
           linewidth=lw)
    x_ticks = x[0:2] + [xx-shift/2 for xx in x[2:-1]] + [10]
    ax.set_xticks(x_ticks)
    ax.set_xticklabels(data['alg'])
    x = x[0:2] + [xx-shift for xx in x[2:-1]] + [10]
    ax.bar(x, data['throughput'], edgecolor=hc, hatch=hatch, color=colors, width=width, linewidth=lw)
    ax.bar(x, data['throughput'], edgecolor=hc, color='none', width=width, linewidth=lw)
    ax.tick_params(axis='x', rotation=20)
    ax.yaxis.grid(linestyle='solid')
    ax.set_axisbelow(True)
    plt.yscale('log')
    plt.yticks([0.001,0.01,0.1,1,10,100])
    plt.ylabel('Average Join Throughput\n[M rec/s]')
    plt.xlabel('')
    handles = [
        Patch(facecolor='white', edgecolor=hc, label='native CPU'),
        Patch(facecolor='#b9bbb6', edgecolor=hc, label='TEE', hatch='//')
    ]
    fig.legend(handles=handles, ncol=6, bbox_to_anchor=(0.95, 1.0),
               fontsize=10, handletextpad=0.3, columnspacing=1,
               frameon=False)
    # plt.subplots_adjust(0,0,1,1,0,0)
    commons.savefig(png_file, tight_layout=True)


if __name__ == '__main__':

    config = commons.read_config(sys.argv)

    if config['experiment']:
        print('experiment')
        # commons.remove_file(csv_file)

    plot()
