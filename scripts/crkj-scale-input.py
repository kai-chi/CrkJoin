#!/usr/bin/python3
import yaml

import commons
import re
import statistics
import subprocess
import csv
import matplotlib.pyplot as plt

csv_data = 'data/crkj-scale-input.csv'
img_path = 'img/crkj-scale-input.png'


def run_join(mode, alg, dsName, sizeM_r, sizeM_s, threads, reps, memory):
    f = open(csv_data, 'a')
    results = []
    cycles_results = []

    s = './app ' + " -a " + alg + " -r " + str(sizeM_r * 1000000) \
        + " -s " + str(sizeM_s * 1000000) + " -n " + str(threads) \
        + ' -m ' + str(memory)
    if 'dynamic' in mode:
        s += ' -b -1 '
    else:
        s += ' -b 13 '

    for i in range(reps):
        stdout = subprocess.check_output(s, cwd="../", shell=True) \
            .decode('utf-8')
        for line in stdout.splitlines():
            if "Throughput" in line:
                throughput = re.findall("\d+\.\d+", line)[1]
                results.append(float(throughput))
                print ("Throughput = " + str(throughput))
            elif "CPU Cycles-per-tuple" in line:
                cycles = int(re.findall(r'\d+', line)[-2])
                cycles_results.append(int(cycles))

    res = statistics.median(results)
    cyc = statistics.median(cycles_results)
    if 'static' in mode:
        alg += '_static'

    s = (mode + "," + alg + "," + str(threads) + "," + dsName + ',' + str(sizeM_r) +
         "," + str(sizeM_s) + "," + str(round(res,2)) + "," + str(cyc))
    print ("AVG : " + s)
    f.write(s + "\n")
    f.close()


def plot():
    csvf = open(csv_data, mode='r')
    csvr = csv.DictReader(csvf)
    all_data = list(csvr)
    algos = sorted(set(map(lambda x:x['alg'], all_data)))
    datasets = sorted(set(map(lambda x:x['dsName'], all_data)))
    max_throughput = max(set(map(lambda x:float(x['throughput']), all_data)))
    fig = plt.figure(figsize=(7,1.2))
    to_datasets = [[y for y in all_data if y['dsName'] == x] for x in datasets]
    for i in range(0, len(datasets)):
        ds = datasets[i]
        data = to_datasets[i]
        to_algos = [[y for y in data if y['alg'] == x] for x in algos]
        plt.subplot(1,2, i+1)
        for j in range(len(algos)):
            alg = algos[j]
            if alg == 'CRKJ_static':
                order = 1
            else:
                order = 2
            r = sorted(list(map(lambda x: int(x['M_R']), to_algos[j])))
            plt.plot(r, list(map(lambda x: float(x['throughput']), to_algos[j])),
                     label=algos[j], color=commons.color_alg(algos[j]), linewidth=2,
                     marker=commons.marker_alg(algos[j]), markeredgecolor='black', markersize=6,
                     markeredgewidth=0.5, zorder=order)
        plt.xscale('log', base=2)
        r = [1,2,4,8,16,32,64,128,256,512,1024,2048,4096]
        plt.ylim([0, 1.1*max_throughput])
        plt.xlabel('|R| [M tuples]', labelpad=-1)

        if i == 0:
            plt.ylabel("Throughput\n[M rec / sec]")
            # r = [1,2,4,8,16,32,64,128,256,429]
            r = [2,8,32,128,429]
            plt.xticks(r, r)
        else:
            # r = [1,2,4,8,16,32,64,128,256,512,1024,2048,4096]
            r = [1,4,16,64,256,1024,4096]
            plt.xticks(r, r)
        plt.gca().yaxis.grid(linestyle='dashed')
        plt.yticks([25,50,75])

        lines, labels = fig.axes[-1].get_legend_handles_labels()
        labels, lines = zip(*sorted(zip(labels, lines), key=lambda t: t[0]))
        fig.legend(lines, labels, fontsize=11, frameon=0,
               ncol=8, bbox_to_anchor = (0.15, 0.845), loc='lower left',
               borderaxespad=0, handletextpad=0.25, columnspacing=1.5)
        plt.title('(' + chr(97+i) + ") Dataset $\it{" + ds + "}$", y=-0.62)

    commons.savefig(img_path, tight_layout=False)


if __name__ == '__main__':
    timer = commons.start_timer()

    mode = 'sgx'

    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    if config['experiment']:
        commons.remove_file(csv_data)
        commons.remove_file(img_path)
        commons.init_file(csv_data, "mode,alg,threads,dsName,M_R,M_S,throughput,cycles-per-tuple\n")

        # run CRKJ and MCJoin for both modes
        algs = ['CrkJoin', 'MCJoin', 'PaMeCo']
        commons.compile_app('sgx')
        for alg in algs:
            threads = config.get('threads', {}).get(alg, 1)
            reps = config.get('reps', 1)
            memory = config.get('memory', {}).get(alg, 0)
            # dataset S=10R
            for r in [1,2,4,8,16,32,64,128]:
                run_join(mode, alg, 'S=10R', r, 10*r, threads, reps, memory)
            # dataset S=R
            for r in [1,2,4,8,16,32,64,128,256,512]:
                run_join(mode, alg, 'S=R', r, r, threads, reps, memory)

        # run RHO
        alg = 'RHO'
        threads = config.get('threads', {}).get(alg, 1)
        reps = config.get('reps', 1)
        memory = config.get('memory', {}).get(alg, 0)
        enclave_string = 'Enclave/Enclave32GB.config.xml'
        commons.compile_app('sgx', config=enclave_string)
        # dataset S=10R
        for r in [1,2,4,8,16,32,64,128]:
            run_join(mode, alg, 'S=10R', r, 10*r, threads, reps, memory)
        # dataset S=R
        for r in [1,2,4,8,16,32,64,128,256,512]:
            run_join(mode, alg, 'S=R', r, r, threads, reps, memory)


    plot()

    commons.stop_timer(timer)
