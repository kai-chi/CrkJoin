#!/usr/bin/python3
import yaml

import commons
import re
import statistics
import subprocess
import csv
import matplotlib.pyplot as plt

res_file = "data/crkj-scale-r.csv"
mb_of_data = 131072


def run_join(alg, size_r, size_s, threads, bits, reps, mode):
    f = open(res_file, "a")
    results = []
    for i in range(0,reps):
        stdout = subprocess.check_output(commons.PROG + " -a " + alg + " -r " + str(size_r) + " -s " + str(size_s) +
                                         " -n " + str(threads) + " - b " + str(bits), cwd="../",shell=True).decode('utf-8')
        for line in stdout.splitlines():
            if "Throughput" in line:
                throughput = re.findall("\d+\.\d+", line)[1]
                s = (mode + "," + alg + "," + str(threads) + "," + str(size_r) + "," + str(size_s) + "," + str(throughput))
                results.append(float(throughput))
                print (s)
    res = statistics.median(results)
    s = (mode + "," + alg + "," + str(threads) + "," + str(size_r) + "," + str(size_s) + "," + str(round(res,2)))
    print ("AVG : " + s)
    f.write(s + "\n")
    f.close()


def plot(partitions):
    csvf = open(res_file, mode='r')
    csvr = csv.DictReader(csvf)
    all_data = list(csvr)
    lines = [0.25 * mb_of_data * partitions,
             2 * mb_of_data * partitions,
             16 * mb_of_data * partitions]
    r_sizes = sorted(set(map(lambda x:int(x['sizeR']), all_data)))
    s_sizes = sorted(set(map(lambda x:int(x['sizeS']), all_data)))
    splitted = [[y for y in all_data if y['sizeS'] == str(x)] for x in s_sizes]
    fig = plt.figure()
    for i in range(0, len(s_sizes)):
        plt.plot(sorted(set(map(lambda x:int(x['sizeR']), splitted[i]))),
                 list(map(lambda x:float(x['throughput']), splitted[i])), label=str(s_sizes[i]/1000000)+'M',
                 linewidth=2, marker='o')
    plt.xlabel('R cardinality')
    plt.ylabel('Throughput [M rec/s]')
    plt.gca().yaxis.grid(linestyle='dashed')
    for line in lines:
        plt.axvline(x=line, linestyle='--', color='#209bb4', linewidth=2)
    plt.legend()
    commons.savefig('img/crkj-scale-r.png')


if __name__ == '__main__':
    timer = commons.start_timer()
    reps = 1
    threads = 1
    mode = "sgx"
    bits = 128

    s_sizes = [160000000,
               320000000,
               640000000,
               1280000000,
               2560000000]

    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    if config['experiment']:
        commons.remove_file(res_file)
        commons.init_file(res_file, "mode,alg,threads,sizeR,sizeS,throughput\n")
        commons.compile_app('sgx')
        for s_size in s_sizes:
            r_size_init = int(mb_of_data * bits / 8)
            stops_per_factor = 4
            min_elems_factor = 1
            max_elems_factor = 7
            min = stops_per_factor * min_elems_factor
            max = stops_per_factor * max_elems_factor
            for i in range (min, max+1):
                r_size = pow(2, i / stops_per_factor)
                r_size *= r_size_init
                if r_size <= s_size:
                    run_join('CRKJ', int(r_size), s_size, threads, bits, reps, 'sgx')

    plot(bits)
    commons.stop_timer(timer)
