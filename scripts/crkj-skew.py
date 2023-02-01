#!/usr/bin/python3

import commons
import subprocess
import csv
import matplotlib.pyplot as plt
import re
import yaml


def run_join(prog, mode, alg, ds, threads, skew, memory, reps, fname):
    f = open(fname, "a")
    res = 0
    for i in range(0, reps):
        app = prog + " -a " + alg + " -d " + ds + " -n " + str(threads) \
              + " -z " + str(skew) + ' -m ' + str(memory)
        stdout = subprocess.check_output(app, cwd="../",shell=True).decode('utf-8')
        for line in stdout.splitlines():
            if "Throughput" in line:
                throughput = re.findall("\d+\.\d+", line)[1]
                s = (mode + "," + alg + "," + ds + "," + str(threads) + "," + str(throughput))
                res += float(throughput)

    res /= reps
    s = (mode + "," + alg + "," + ds + "," + str(skew) + "," + str(round(res, 2)))
    print(s)
    f.write(s + "\n")
    f.close()


def plot(fname):
    plot_filename = "img/crkj-skew.png"
    csvf = open(fname, mode='r')
    csvr = csv.DictReader(csvf)
    all_data = list(csvr)
    algos = sorted(set(map(lambda x:x['alg'], all_data)))
    datasets = sorted(set(map(lambda x:x['ds'], all_data)), reverse=False)
    max_throughput = max(set(map(lambda x:float(x['throughput']), all_data)))
    fig = plt.figure(figsize=(7,2.4))
    to_datasets = [[y for y in all_data if y['ds'] == x] for x in datasets]
    for d in range(0, len(datasets)):
        ds = datasets[d]
        to_algos = [[y for y in to_datasets[d] if y['alg'] == x] for x in algos]
        plt.subplot(1,2,d+1)
        plt.gca().yaxis.grid(linestyle='dashed')
        for a in range(0, len(algos)):
            alg = algos[a]
            skews = list(map(lambda x:x['skew'], to_algos[a]))
            skews = [0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1]
            plt.plot(skews, list(map(lambda x:float(x['throughput']), to_algos[a]))[0:7],
                     label=alg, color=commons.color_alg(alg), linewidth=2,
                     marker=commons.marker_alg(algos[a]), markeredgecolor='black', markersize=6,
                     markeredgewidth=0.5)
        fig.text(0.54, 0.28, 'Zipf factor', ha='center')
        if d == 0:
            plt.ylabel("Throughput [M rec / s]")
        plt.ylim([0, 1.1*max_throughput])
        lines, labels = fig.axes[-1].get_legend_handles_labels()
        fig.legend(lines, labels, fontsize=9, frameon=0,
               ncol=8, bbox_to_anchor = (0.13, 0.92), loc='lower left', borderaxespad=0,
               handletextpad=0.25, columnspacing=1.5, handlelength=2)
        plt.title('(' + chr(97+d) + ") Dataset $\it{" + datasets[d] + "}$", y=-0.45)

    commons.savefig(plot_filename)
    

if __name__ == '__main__':
    timer = commons.start_timer()
    skews = [0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1, 1.1, 1.2, 1.3]
    res_file = "data/crkj-skew.csv"
    mode = 'sgx'

    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    if config['experiment']:
        reps = config.get('reps', 1)
        enclave_string = 'Enclave/Enclave4GB.config.xml'
        commons.compile_app(mode, config=enclave_string)
        commons.remove_file(res_file)
        commons.init_file(res_file, "mode,alg,ds,skew,throughput\n")

        for alg in ['CrkJoin', 'RHO', 'MCJoin', 'PaMeCo']:
            memory = config.get('memory', {}).get(alg, 0)
            threads = config.get('threads', {}).get(alg, 1)
            for ds in commons.get_join_dataset_names():
                for skew in skews:
                    run_join(commons.PROG, mode, alg, ds, threads, skew, memory, reps, res_file)

    plot(res_file)
    commons.stop_timer(timer)
