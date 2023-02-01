#!/usr/bin/python3
import yaml

import commons
import re
import statistics
import subprocess
import csv
import matplotlib.pyplot as plt

csv_data = "data/crkj-scale-build-phase.csv"
mb_of_data = 131072


def run_join(mode, alg, dsName, size_r, threads, reps):
    f = open(csv_data, 'a')
    results = []

    for i in range(reps):
        stdout = subprocess.check_output('./app ' + " -a " + alg + " -r " + str(size_r)
                                         + " -s 0 " + " -n " + str(threads),
                                         cwd="../", shell=True) \
            .decode('utf-8')
        for line in stdout.splitlines():
            if "Throughput" in line:
                throughput = re.findall("\d+\.\d+", line)[1]
                results.append(float(throughput))
                print ("Throughput = " + str(throughput))

    res = statistics.median(results)
    s = (mode + "," + alg + "," + str(threads) + "," + dsName + ',' + str(size_r) +
         "," + str(round(res,2)))
    print ("AVG : " + s)
    f.write(s + "\n")
    f.close()


def plot():
    csvf = open(csv_data, mode='r')
    csvr = csv.DictReader(csvf)
    all_data = list(csvr)
    plt.figure()
    r = sorted(list(map(lambda x: int(x['sizeR']), all_data)))
    plt.plot(r, list(map(lambda x: float(x['throughput']), all_data)),
             linewidth=2, marker='o', markeredgecolor='black', markersize=6,
             markeredgewidth=0.5)
    plt.xlabel('|R| [tuples]')
    plt.ylabel("Throughput [M rec / sec]")
    plt.xscale('log')
    plt.gca().yaxis.grid(linestyle='dashed')
    commons.draw_vertical_lines(plt, [mb_of_data*128/4, mb_of_data*128*2, mb_of_data*128*16])
    commons.savefig('img/crkj-scale-build-phase.png')


if __name__ == '__main__':
    timer = commons.start_timer()

    threads = 1

    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    if config['experiment']:
        commons.remove_file(csv_data)
        commons.init_file(csv_data, "mode,alg,threads,dsName,sizeR,throughput\n")

        for mode in ['sgx']:
            commons.compile_app(mode)
            stops_per_factor = 4
            min_elems_factor = 1
            max_elems_factor = 10
            min = stops_per_factor * min_elems_factor
            max = stops_per_factor * max_elems_factor
            for i in range(min, max+1):
                r_size = (int(pow(2, i/stops_per_factor) * 1000000))
                run_join(mode, 'CRKJ', 'S=0', r_size, threads, config['reps'])

    plot()

    commons.stop_timer(timer)
