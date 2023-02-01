#!/usr/bin/python3
import itertools
import subprocess
import re
import statistics
import matplotlib.pyplot as plt

import commons
import yaml
import pandas as pd

csv_file = "data/crkj-bits-output.csv"
png_file = "img/crkj-bits.png"


def run_join(alg, threads, dataset, sizeR, sizeS, bits, reps):
    f = open(csv_file, "a")
    results = []
    cycles_results = []
    R = 1000000 * int(sizeR)
    S = 1000000 * int(sizeS)

    for i in range(0,reps):
        s = "./app -a " + str(alg) + ' -n ' + str(threads) + \
            " -r " + str(R) + " -s " + str(S) + \
            " -b " + str(bits)
        try:
            stdout = subprocess.check_output(s, cwd="../",shell=True, timeout=15*60)\
                .decode('utf-8')
        except subprocess.TimeoutExpired as exc:
            print("Command timed out: {}".format(exc))
            s = (alg + ',' + str(threads) + "," + dataset + ',' + str(sizeR) + ',' + str(sizeS) + "," + str(bits) +
                 "," + str(0) + "," + str(999))
            print ("AVG : " + s)
            f.write(s + "\n")
            f.close()
            return

        print(stdout)
        for line in stdout.splitlines():
            if "Throughput" in line:
                throughput = re.findall("\d+\.\d+", line)[1]
                results.append(float(throughput))
            elif "CPU Cycles-per-tuple" in line:
                cycles = int(re.findall(r'\d+', line)[-2])
                cycles_results.append(int(cycles))

    res = statistics.median(results)
    cyc = statistics.median(cycles_results)
    s = (alg + ',' + str(threads) + "," + dataset + ',' + str(sizeR) + ',' + str(sizeS) + "," + str(bits) +
         "," + str(round(res,2)) + "," + str(cyc))
    print ("AVG : " + s)
    f.write(s + "\n")
    f.close()


def plot():
    data = pd.read_csv(csv_file)
    datasets = data['dataset'].unique()
    number_of_rows = 1
    fig = plt.figure(figsize=(10.7,1.7*number_of_rows))
    ms = 3.5
    i = 0
    optimal_bits = {
        4:4,8:5,16:6,32:7,64:8,128:9,256:10,512:11
    }

    for ds in datasets:
        plt.subplot(number_of_rows,int(len(datasets)/number_of_rows)+1, i+1)
        marker = itertools.cycle(('v','^','<','>','8','s','p','P','*','D','H','X'))
        d = data.loc[data['dataset'] == ds]
        for key, grp in d.groupby('sizeR'):
            l = str((int(key))) + 'M'
            tmpMarker = next(marker)
            ax = grp.plot(ax=plt.gca(),x='bits',y='cycles-per-tuple',
                          label=l, marker=tmpMarker,
                          markersize=ms,
                          legend=False,
                          linestyle='--', linewidth=1,
                          color='gray', zorder=1)
            row = grp.loc[grp['cycles-per-tuple'].idxmin()]
            plt.plot(row['bits'], row['cycles-per-tuple'], markersize=ms+0.1,
                     marker=tmpMarker, color='#de3d82', zorder=2)
            bit = optimal_bits[int(key)]
            row = grp.loc[grp['bits'] == bit]
            if not row.empty:
                plt.scatter(bit, row['cycles-per-tuple'], facecolors='none',
                            edgecolors='#72e06a', zorder=3, s=50,
                            linewidth=1.5)

        plt.title('(' + chr(97+i) + ") Dataset $\it{" + ds + "}$", y=-0.41)
        plt.xlabel('Partitioning bits', labelpad=-3)
        plt.ylim([0, 500])
        if i == 0:
            plt.ylabel('CPU cycles per tuple')
            lines, labels = fig.axes[-1].get_legend_handles_labels()
        else:
            plt.gca().set(yticklabels=[])
        plt.gca().xaxis.grid(linestyle='dashed')
        i = i + 1

    ph = [plt.plot([],marker="", ls="")[0]]
    lines = ph + lines
    labels = ["R size:"] + labels
    fig.legend(lines, labels, fontsize=8, frameon=0,
               ncol=9, bbox_to_anchor=(0.13, 0.86), loc='lower left',
               borderaxespad=0, handletextpad=0.25, columnspacing=1.3)
    commons.savefig(png_file, tight_layout=False)


if __name__ == '__main__':

    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    threads = 1
    bits = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16]
    size = [4,8,16,32,64,128,256,512]

    if config['experiment']:
        commons.remove_file(csv_file)
        commons.init_file(csv_file, "alg,threads,dataset,sizeR,sizeS,bits,throughput,cycles-per-tuple\n")

        commons.compile_app('sgx', config='Enclave/Enclave4GB.config.xml')
        for s in size:
            for i in bits:
                if s >= 64 and i < 5:
                    pass
                else:
                    run_join('CRKJ', threads, 'S=R', s, s, i, config['reps'])
        for s in size:
            for i in bits:
                if s >= 64 and i < 5:
                    pass
                else:
                    run_join('CRKJ', threads, 'S=10R', s, 10*s, i, config['reps'])
        for s in size:
            for i in bits:
                if s >= 64 and i < 5:
                    pass
                else:
                    run_join('CRKJ', threads, 'S=50R', s, 50*s, i, config['reps'])

        for s in size:
            for i in bits:
                if s >= 64 and i < 5:
                    pass
                else:
                    run_join('CRKJ', threads, '10S=R', 10*s, s, i, config['reps'])
        for s in size:
            for i in bits:
                if s >= 64 and i < 5:
                    pass
                else:
                    run_join('CRKJ', threads, '50=R', 50*s, s, i, config['reps'])

    plot()
