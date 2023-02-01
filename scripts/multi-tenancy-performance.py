#!/usr/bin/python3
import os

import pandas as pd
from matplotlib import pyplot as plt
from matplotlib.gridspec import GridSpec

from scripts import commons

delays_csv = "data/multi-query-delays.csv"
png_file = "img/multi-tenancy-performance.png"
queries_csv = "data/multi-query-queries.csv"


def plot():
    fs_legend = 12
    fs_titles = 12
    pos_title = -0.38
    h_legend = 0.97
    aax = []
    fig = plt.figure(figsize=(10,2.2))
    gs1 = GridSpec(1, 2, figure=fig)
    gs2 = GridSpec(1, 2, figure=fig)
    gs1.update(left=0.05, right=0.47, wspace=0.05)
    gs2.update(left=0.55, right=0.98, wspace=0.05)
    aax.append(fig.add_subplot(gs1[0,0]))
    aax.append(fig.add_subplot(gs1[0,1]))
    aax.append(fig.add_subplot(gs2[0,0]))
    aax.append(fig.add_subplot(gs2[0,1]))

    ms=5
    mew=0.3
    lw=1.8
    delays_data = pd.read_csv(delays_csv)
    datasets = delays_data['dataset'].unique()
    for i in [2,3]:
        ds = datasets[i-2]
        data = delays_data.loc[delays_data['dataset'] == ds]
        ax = aax[i]
        for key, grp in data.groupby('alg'):
            grp.plot(ax=ax,x='delay',y='throughput', label=key,
                     marker=commons.marker_alg(key),
                     markersize=ms, legend=False,
                     color=commons.color_alg(key),
                     markeredgecolor='black', markeredgewidth=mew,
                     linewidth=lw, zorder=2)
        if i == 2:
            ax.set_ylabel("Avg query throughput\n[M rec / sec]",
                          fontsize=fs_titles,
                          labelpad=-0.1)
            ax.axhline(y=84.06, color=commons.color_alg('CrkJoin'), linestyle='--',zorder=1)
            ax.axhline(y=25.71, color=commons.color_alg('RHO'), linestyle='--',zorder=1)
            ax.axhline(y=19.53, color=commons.color_alg('MCJoin'), linestyle='--',zorder=1)
            ax.axhline(y=13.20, color=commons.color_alg('PaMeCo'), linestyle='--',zorder=1)

        else:
            ax.set(yticklabels=[])
            ax.axhline(y=78.77, color=commons.color_alg('CrkJoin'), linestyle='--',zorder=1)
            ax.axhline(y=26.86, color=commons.color_alg('RHO'), linestyle='--',zorder=1)
            ax.axhline(y=10.88, color=commons.color_alg('MCJoin'), linestyle='--',zorder=1)
            ax.axhline(y=22.4, color=commons.color_alg('PaMeCo'), linestyle='--',zorder=1)
            lines, labels = ax.get_legend_handles_labels()
            leg = ax.legend(lines, labels, fontsize=fs_legend, frameon=0,
                            ncol=4, bbox_to_anchor=(-1.2, h_legend), loc='lower left',
                            borderaxespad=0, handletextpad=0.25, columnspacing=1.5,
                            markerscale=1.5)
            for legobj in leg.legendHandles:
                legobj.set_linewidth(3.0)
        ax.set_xlabel('Delay  [ms]', labelpad=-1,
                      fontsize=fs_titles)
        ax.yaxis.grid(linestyle='dashed')
        ax.set_axisbelow(True)
        ax.set_ylim([-5, 1.1 * delays_data['throughput'].max()])

        ax.set_title('(' + chr(97+i) + ") Dataset $\it{" + ds + "}$", y=pos_title)
        # if i == 1:
        #     plt.subplots_adjust(wspace=None)

    queries_data = pd.read_csv(queries_csv)
    datasets = queries_data['dataset'].unique()
    for i in [0,1]:
        ds = datasets[i]
        data = queries_data.loc[queries_data['dataset'] == ds]
        ax = aax[i]
        crkj_throughput = data.loc[data['alg'] == 'CrkJoin']['throughput'].to_list()
        queries = data.loc[data['alg'] == 'CrkJoin']['queries'].to_list()
        # improvement over RHO
        rho_throughput = data.loc[data['alg'] == 'RHO']['throughput'].to_list()
        l = min(len(crkj_throughput), len(rho_throughput))
        improvement = [a/b for a,b in zip(crkj_throughput[:l], rho_throughput[:l])]
        ax.plot(queries[:l], improvement, label='over RHO',
                 marker=commons.marker_alg('RHO'), color=commons.color_categorical(0),
                 markersize=ms, markeredgecolor='black',
                 markeredgewidth=mew, linewidth=lw)

        # improvement over MCJoin
        mcjoin_throughput = data.loc[data['alg'] == 'MCJoin']['throughput'].to_list()
        l = min(len(crkj_throughput), len(mcjoin_throughput))
        improvement = [a/b for a,b in zip(crkj_throughput[:l], mcjoin_throughput[:l])]
        ax.plot(queries[:l], improvement, label='over MCJoin',
                 marker=commons.marker_alg('MCJoin'), color=commons.color_categorical(1),
                 markersize=ms, markeredgecolor='black',
                 markeredgewidth=mew, linewidth=lw)

        # improvement over PaMeCo
        pameco_throughput = data.loc[data['alg'] == 'PaMeCo']['throughput'].to_list()
        l = min(len(crkj_throughput), len(pameco_throughput))
        improvement = [a/b for a,b in zip(crkj_throughput[:l], pameco_throughput[:l])]
        ax.plot(queries[:l], improvement, label='over PaMeCo',
                 marker=commons.marker_alg('PaMeCo'), color=commons.color_categorical(2),
                 markersize=ms, markeredgecolor='black',
                 markeredgewidth=mew, linewidth=lw)

        ax.set_xlabel('Concurrent queries', labelpad=-1,
                      fontsize=fs_titles)
        ax.set_yscale('log')
        ax.yaxis.grid(linestyle='dashed')
        if i == 0:
            ax.set_ylabel("CrkJoin improvement",
                          fontsize=fs_titles,
                          labelpad=-0.1)
            lines, labels = ax.get_legend_handles_labels()
            leg = ax.legend(lines, labels, fontsize=fs_legend, frameon=0,
                       ncol=3, bbox_to_anchor=(-0.2, h_legend), loc='lower left',
                       borderaxespad=0, handletextpad=0.25, columnspacing=1.5,
                            markerscale=1.5)
            for legobj in leg.legendHandles:
                legobj.set_linewidth(3.0)
        else:
            ax.set(yticklabels=[])

        ax.set_title('(' + chr(97+i) + ") Dataset $\it{" + ds + "}$", y=pos_title)

    commons.savefig(png_file, tight_layout=True)


if __name__ == '__main__':

    # os.system('./multi-query-delays.py')
    # os.system('./multi-query-queries.py')

    plot()
