#!/usr/bin/python3
import pandas as pd
from matplotlib.gridspec import GridSpec

import commons
import csv
import matplotlib.pyplot as plt
import yaml


def plot():
    fname_skew = "data/crkj-skew.csv"
    fname_scales = "data/crkj-scale-s.csv"
    plot_filename = "img/crkj-data-twists.png"
    skew_data  = pd.read_csv(fname_skew)
    max_throughput = skew_data['throughput'].max()
    title_pos = -0.45
    xlabel_pos = -0.22
    ylable_pos = -0.1
    fs_labels = 11
    fig = plt.figure(figsize=(2.5*3,1.7))
    gs1 = GridSpec(1, 2, figure=fig)
    gs2 = GridSpec(1, 1, figure=fig)
    gs1.update(left=0.05, right=0.68, wspace=0.05)
    gs1.update(wspace=0.06, hspace=0.5) # set the spacing between axes.
    gs2.update(left=0.78, right=0.98, wspace=0.05)
    i=0
    for dkey, dgrp in skew_data.groupby('ds'):
        ax = fig.add_subplot(gs1[0, i])
        for akey, agrp in dgrp.groupby('alg'):
            agrp = agrp[agrp['skew']<= 1]
            agrp.plot(ax=ax, x = 'skew', y='throughput', label=akey,
                      color=commons.color_alg(akey), marker=commons.marker_alg(akey),
                      markeredgecolor='black',markeredgewidth=0.5, legend=False)
        ax.set_xlabel('Zipf factor', labelpad=xlabel_pos, fontsize=fs_labels)
        ax.yaxis.grid(linestyle='dashed')
        ax.set_ylim([0, 1.1*max_throughput])
        # ax.set_xlim([0.35, 1.05])
        if i == 0:
            ax.set_ylabel("Throughput [M rec / s]", labelpad=ylable_pos, fontsize=fs_labels-1)
        else:
            ax.set(yticklabels=[])
        ax.set_title('(' + chr(97+i) + ") Skew - Dataset $\it{" + dkey + "}$", y=title_pos)
        i = i + 1
    lines, labels = ax.get_legend_handles_labels()
    for l in lines:
        l.set_linewidth(2.5)
    fig.legend(lines, labels, fontsize=9, frameon=0,
               ncol=8, bbox_to_anchor = (0.04, 0.86), loc='lower left', borderaxespad=0,
               handletextpad=0.25, columnspacing=1.5, handlelength=2,
               markerscale=1)
    scale_data = pd.read_csv(fname_scales)
    max = scale_data['throughput'].max()
    ax = fig.add_subplot(gs2[0,0])
    i = 0
    for key, grp in scale_data.groupby('sizeR'):
        grp.plot(ax=ax, x='ratioS',y='throughput',
                 label=str(int(key)/1000000), marker='o',
                 color=commons.color_categorical(i), legend=False)
        i = i + 1

    ax.set_ylim([0, 1.1*max])
    ax.set_xlabel('|S|:|R| ratio', labelpad=xlabel_pos, fontsize=fs_labels)
    ax.set_ylabel("CrkJoin Throughput\n[M rec / sec]", labelpad=ylable_pos, fontsize=fs_labels)
    plt.xticks([1,5,10])
    ax.yaxis.grid(linestyle='dashed')
    lines, labels = ax.get_legend_handles_labels()
    # lines = [plt.plot([],marker="", ls="")[0]] + lines
    labels = [str(int(float(x))) + 'M' for x in labels]
    # labels = ['R size:'] + labels
    # fig.legend(lines, labels, fontsize=10, frameon=0,
    #            ncol=3, bbox_to_anchor=(0.7, 0.92), loc='lower left',
    #            borderaxespad=0, handletextpad=1, columnspacing=1,
    #            markerscale=1)
    ax.legend(lines, labels, loc=4, prop={'size': 8},ncol=2,columnspacing=1, handletextpad=0.5, title='R size')
    ax.set_title('(' + chr(97+2) + ") Cardinatities ratio", y=title_pos)
    commons.savefig(plot_filename, tight_layout=False)
    

if __name__ == '__main__':
    plot()
