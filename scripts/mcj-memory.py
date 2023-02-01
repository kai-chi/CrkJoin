#!/usr/bin/python3

import subprocess
import re
import statistics
import matplotlib as mpl
import matplotlib.pyplot as plt
import csv
import commons
import yaml

csv_file = "data/mcj-memory-output.csv"
png_file = "img/mcj-memory.png"


def run_join(alg, sizeR, sizeS, memory, reps):
    f = open(csv_file, "a")
    results = []

    for i in range(0,reps):
        s = "./app -a " + str(alg) + " -r " + str(sizeR) + " -s " + str(sizeS) + " -m " + str(memory) + ' -n 1 '
        stdout = subprocess.check_output(s, cwd="../",shell=True)\
            .decode('utf-8')
        print(stdout)
        for line in stdout.splitlines():
            if "Throughput" in line:
                throughput = re.findall("\d+\.\d+", line)[1]
                results.append(float(throughput))

    res = statistics.median(results)
    s = (alg + "," + str(sizeR) + ',' + str(sizeS) + "," + str(memory) + "," + str(round(res,2)))
    print ("AVG : " + s)
    f.write(s + "\n")
    f.close()


def plot():
    csvf = open(csv_file, mode='r')
    csvr = csv.DictReader(csvf)
    all_data = list(csvr)
    # now print the same thing but only SGX
    mpl.rcParams.update(mpl.rcParamsDefault)
    fig = plt.figure(figsize=(7,2.4))

    memory = list(map(lambda x : int(x['memory']), all_data))
    throughputs = list(map(lambda x: float(x['throughput']), all_data))
    plt.plot(memory, throughputs, '-o')
    # make the figure neat
    plt.ylabel("Throughput [M rec / sec]")
    # plt.xticks([2,4,6,8])
    # plt.yticks([0,10,20,30,40])
    # plt.ylim(top=40, bottom=-1)
    plt.gca().yaxis.grid(linestyle='dashed')
    plt.xlabel('Number of bits')
    # save fig
    commons.savefig(png_file)


if __name__ == '__main__':

    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    sizeR = 32000000
    sizeS = 320000000

    if config['experiment']:
        commons.remove_file(csv_file)
        commons.init_file(csv_file, "alg,sizeR,sizeS,memory,throughput\n")

        commons.compile_app('sgx')
        for i in range(80, 300, 5):
            run_join('MCJ', sizeR, sizeS, i, config['reps'])

    plot()
