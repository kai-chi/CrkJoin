#include "JoinWrapper.hpp"
#include "Join.hpp"
#include <pthread.h>
#include <string>

#ifdef NATIVE_COMPILATION
#include "Logger.h"
#include "native_ocalls.h"
#include <cassert>
#include <malloc.h>
#include <pthread.h>

#else
#include "Enclave.h"
#include "Enclave_t.h"
#endif

struct crk_arg_t
{
    int tid;
    Join *crkJoin;
    threadresult_t *threadresult;
}; // __attribute__((aligned(CACHE_LINE_SIZE)));

typedef void (Join::*CrkjJoinFunction)(const int threadID,
                                      threadresult_t *threadresult,
                                      bool crackFlag);

typedef void * (*threadFunction) (void *);

static void *
run (void * args)
{
    auto * arg = reinterpret_cast<crk_arg_t*>(args);
    arg->crkJoin->join(arg->tid, arg->threadresult, false);
    return nullptr;
}

static void *
run_simple (void * args)
{
    auto * arg = reinterpret_cast<crk_arg_t*>(args);
    arg->crkJoin->join_simple_dfs_st(arg->tid, arg->threadresult, false);
    return nullptr;
}

static void *
run_fusion (void * args)
{
    auto * arg = reinterpret_cast<crk_arg_t*>(args);
    arg->crkJoin->joinFusion(arg->tid, arg->threadresult, false);
    return nullptr;
}

struct arg_t_part {
    int32_t my_tid;
    Join *crkJoin;
    PTreeNode *node;
    PTreeNode *root;
    int left;
    int crackingThreshold;

}; // __attribute__((aligned(CACHE_LINE_SIZE)));

void *
partition_thread(void * param)
{
    auto * args = (arg_t_part*) param;
    if (args->left <= 0 || !args->node) {
        return nullptr;
    }
    args->crkJoin->partition_both_ends(args->node,
                                       args->root,
                                       0);

    if (args->left - 1 <= 0) return 0;
    pthread_t * threads = new pthread_t[2];
    arg_t_part * childArgs = new arg_t_part[2];

    for (int i = 0; i < 2; i++) {
        childArgs[i].my_tid = (2 * args->my_tid) + (i+1);
        childArgs[i].crkJoin = args->crkJoin;
        if (i == 0) {
            childArgs[i].node = args->node->getBranch0();
        } else {
            childArgs[i].node = args->node->getBranch1();
        }
        childArgs[i].root = args->root;
        childArgs[i].left = args->left - 1;

        int rv = pthread_create(&threads[i], nullptr, partition_thread, (void*)&childArgs[i]);
        if (rv) {
            logger(ERROR, "pthread_create return value: %d", rv);
            ocall_throw("");
        }
    }
    for (int i = 0; i < 2; i++) {
        pthread_join(threads[i], nullptr);
    }

    delete[] threads;
    delete[] childArgs;
    return nullptr;
}

void *
partition_thread_simple(void * param)
{
    auto * args = (arg_t_part*) param;
    if (args->left <= 1 || !args->node) {
        //do not create more threads but finish partitioning on your own
        args->crkJoin->crack_dfs(args->node, args->root, args->crackingThreshold);
        return nullptr;
    }

    args->crkJoin->partition_both_ends(args->node,
                                       args->root,
                                       0);


    auto * threads = new pthread_t[2];
    auto * childArgs = new arg_t_part[2];

    for (int i = 0; i < 2; i++) {
        childArgs[i].my_tid = (2 * args->my_tid) + (i+1);
        childArgs[i].crkJoin = args->crkJoin;
        if (i == 0) {
            childArgs[i].node = args->node->getBranch0();
        } else {
            childArgs[i].node = args->node->getBranch1();
        }
        childArgs[i].root = args->root;
        childArgs[i].left = args->left - 1;

        int rv = pthread_create(&threads[i], nullptr, partition_thread_simple, (void*)&childArgs[i]);
        if (rv) {
            logger(ERROR, "pthread_create return value: %d", rv);
            ocall_throw("");
        }
    }
    for (int i = 0; i < 2; i++) {
        pthread_join(threads[i], nullptr);
    }

    delete[] threads;
    delete[] childArgs;
    return nullptr;
}

static inline uint32_t _log2(const uint32_t x) {
    uint32_t y;
    asm ( "\tbsr %1, %0\n"
    : "=r"(y)
    : "r" (x)
    );
    return y;
}

static uint32_t
getRadixBits(uint64_t numTuples, joinconfig_t * config)
{
    uint32_t bits;
    if (config->RADIXBITS == -1) {
        uint64_t recordsInL2 = (L2_CACHE_SIZE * 1024/ sizeof(type_key));
//        bits = (uint32_t)std::ceil(_log2(uint32_t(Commons::nextPowerOfTwo(numTuples / recordsInL2))));
//        bits++;
        double tmp = numTuples * sizeof(tuple_t) / (1024 * L2_CACHE_SIZE);
        bits = (uint32_t) std::ceil(std::log2(tmp));
        logger(INFO, "Dynamic bit selection : %d", bits);
    }
    else {
        logger(INFO, "Static bit selection");
        bits = config->RADIXBITS;
    }
    // make sure the number of partition is not smaller than the number of threads
    while(config->NTHREADS > (1 << bits)) {
        bits++;
    }
    return bits;
}

result_t * CRKJ_st_template(relation_t *relR, relation_t *relS, joinconfig_t * config,
                            CrkjJoinFunction jf, bool crackFlag)
{
    uint64_t timerStart, timerStop;
    uint64_t timerPartitionUsec=0, timerJoinUsec;
    uint32_t bits = getRadixBits(relR->num_tuples, config);
    auto joinresult = new result_t;
    joinresult->resultlist = new threadresult_t[1]();
    logger(INFO, "Number of bits = %d (%d partitions)", bits, (1 << bits));
    Join *crkJoin = new Join(1, bits, relR, relS, config->WRITETOFILE, config->MATERIALIZE);
    ocall_get_system_micros(&timerStart);
    (crkJoin->*jf)(0, &(joinresult->resultlist[0]), crackFlag);

    ocall_get_system_micros(&timerStop);
    timerJoinUsec = timerStop - timerStart;

    join_result_t jr = crkJoin->getJoinResult();

    double throughput = (double) (relR->num_tuples + relS->num_tuples) / (double) (timerPartitionUsec + timerJoinUsec);

    joinresult->throughput = throughput;
    joinresult->totalresults = jr.matches;
    joinresult->nthreads = 1;

    logger(INFO, "CrkJ matches = %lu", jr.matches);
#ifdef TIMERS
    logger(INFO, "Phase build (cycles)     : %lu", jr.timer_ht);
    logger(INFO, "Phase partition (cycles) : %lu", jr.timer_part);
    logger(INFO, "Phase probe (cycles)     : %lu", jr.timer_probe);
    logger(INFO, "Time partition (usec)    : %lu (%.2lf%%)", timerPartitionUsec,
           (100.0 * (double)timerPartitionUsec / (double)(timerPartitionUsec + timerJoinUsec)));
    logger(INFO, "Time join (usec)         : %lu (%.2lf%%)", timerJoinUsec,
           (100.0 * (double)timerJoinUsec / (double)(timerPartitionUsec + timerJoinUsec)));
    logger(INFO, "");
    uint64_t joinTime = jr.join_usec;
    logger(INFO, "Time join_partition      : %lu (%.2lf%%)", jr.join_partition_usec,
           (100.0 * (double)jr.join_partition_usec / (double)joinTime));
    logger(INFO, "Time join_build          : %lu (%.2lf%%)", jr.join_build_usec,
           (100.0 * (double)jr.join_build_usec / (double)joinTime));
    logger(INFO, "Time join_probe          : %lu (%.2lf%%)", jr.join_probe_usec,
           (100.0 * (double)jr.join_probe_usec / (double)joinTime));

#endif
    logger(INFO, "Time total (usec)        : %lu", (timerPartitionUsec + timerJoinUsec));
    logger(INFO, "Throughput (M rec/sec)   : %.4lf", throughput);
#ifdef COUNT_SCANNED_TUPLES
    logger(INFO, "Scanned Tuples           : %lu", jr.scanned_tuples);
#endif
#ifdef MEASURE_PARTITIONS
    std::string res;
    for (int i  = 0; i < (1 << bits); i++) {
        uint64_t t = jr.timer_per_partition[0][i];
        res.append(std::to_string(t));
        if (i != ((1<<bits) - 1)) {
            res.append(",");
        }
    }
    logger(INFO, "TimerPerPartition : %s", res.c_str());
#endif

    delete crkJoin;
    return joinresult;
}

result_t * CRKJ_st(relation_t *relR, relation_t *relS, joinconfig_t * config)
{
    return CRKJ_st_template(relR, relS, config, &Join::join, false);
}

result_t * CRKJ_partition_only(relation_t *relR, relation_t *relS, joinconfig_t * config)
{
    uint64_t timerStart, timerStop;
    uint64_t timerPartitionUsec, timerJoinUsec;
    uint32_t bits = getRadixBits(relR->num_tuples, config);
    auto joinresult = new result_t;
    joinresult->resultlist = new threadresult_t[1]();
    logger(INFO, "Number of bits = %d (%d partitions)", bits, (1 << bits));
    Join *crkJoin = new Join(1, bits, relR, relS, config->WRITETOFILE, config->MATERIALIZE);
    ocall_get_system_micros(&timerStart);

    crkJoin->partition_only();

    ocall_get_system_micros(&timerStop);
    timerJoinUsec = timerStop - timerStart;

    join_result_t jr = crkJoin->getJoinResult();

    joinresult->throughput = 0;
    joinresult->totalresults = jr.matches;
    joinresult->nthreads = 1;

    logger(INFO, "CrkJ matches = %lu", jr.matches);
#ifdef TIMERS
//    logger(INFO, "Phase build (cycles)     : %lu", jr.timer_ht);
//    logger(INFO, "Phase partition (cycles) : %lu", jr.timer_part);
//    logger(INFO, "Phase probe (cycles)     : %lu", jr.timer_probe);
    logger(INFO, "Time partition (usec)    : %lu (%.2lf%%)", timerPartitionUsec,
           (100.0 * (double)timerPartitionUsec / (double)(timerPartitionUsec + timerJoinUsec)));
//    logger(INFO, "Time join (usec)         : %lu (%.2lf%%)", timerJoinUsec,
//           (100.0 * (double)timerJoinUsec / (double)(timerPartitionUsec + timerJoinUsec)));
//    logger(INFO, "");
//    uint64_t joinTime = jr.join_usec;
//    logger(INFO, "Time join_partition      : %lu (%.2lf%%)", jr.join_partition_usec,
//           (100.0 * (double)jr.join_partition_usec / (double)joinTime));
//    logger(INFO, "Time join_build          : %lu (%.2lf%%)", jr.join_build_usec,
//           (100.0 * (double)jr.join_build_usec / (double)joinTime));
//    logger(INFO, "Time join_probe          : %lu (%.2lf%%)", jr.join_probe_usec,
//           (100.0 * (double)jr.join_probe_usec / (double)joinTime));

#endif
    logger(INFO, "Time total (usec)        : %lu", (timerPartitionUsec + timerJoinUsec));
    std::string res;
    for (int i  = 0; i < (1 << bits); i++) {
        uint64_t t = jr.timer_per_partition[0][i];
        res.append(std::to_string(t));
        if (i != ((1<<bits) - 1)) {
            res.append(",");
        }
    }
    logger(INFO, "TimerPerPartition : %s", res.c_str());

    delete crkJoin;
    return joinresult;
}

result_t * CRKJ_template(relation_t *relR, relation_t *relS, joinconfig_t * config,
                         threadFunction partitionFunction, threadFunction runFunction)
{
    uint64_t timerStart, timerStop, cycles;
    uint64_t timerPartitionUsec, timerJoinUsec;
    int nthreads = config->NTHREADS;
    uint32_t bits = getRadixBits(relR->num_tuples, config);
    logger(INFO, "Cardinality R: %lu, S: %lu",
           relR->num_tuples, relS->num_tuples);
    logger(INFO, "Number of bits = %d (%d partitions)", bits, (1 << bits));
    logger(INFO, "Cracking threshold : %d", config->CRACKING_THRESHOLD);
#ifdef PCM_COUNT
    ocall_set_system_counter_state("Join");
#endif
    Join *crkJoin = new Join(nthreads, bits, relR, relS, config->WRITETOFILE, config->MATERIALIZE);
    auto * threads = new pthread_t[nthreads];
    auto * args = new crk_arg_t[nthreads];

    ocall_get_system_micros(&timerStart);
    ocall_startTimer(&cycles);
    pthread_t partThread;
    arg_t_part argsPart{};
    argsPart.my_tid = 0;
    argsPart.crkJoin = crkJoin;
    argsPart.node = crkJoin->getTreeRootR();
    argsPart.root = crkJoin->getTreeRootR();
    argsPart.left = (int) std::log2(nthreads);
    argsPart.crackingThreshold = config->CRACKING_THRESHOLD;
    logger(INFO, "CrkJoin");

    int rv = pthread_create(&partThread, nullptr, partitionFunction, (void*)&argsPart);
    if (rv) {
        logger(ERROR, "pthread_create return value: %d", rv);
        ocall_throw("");
    }
    pthread_join(partThread, nullptr);
    argsPart.my_tid = 0;
    argsPart.crkJoin = crkJoin;
    argsPart.node = crkJoin->getTreeRootS();
    argsPart.root = crkJoin->getTreeRootS();
    argsPart.left = (int) std::log2(nthreads);
    argsPart.crackingThreshold = config->CRACKING_THRESHOLD;

    rv = pthread_create(&partThread, nullptr, partitionFunction, (void*)&argsPart);
    if (rv) {
        logger(ERROR, "pthread_create return value: %d", rv);
        ocall_throw("");
    }
    pthread_join(partThread, nullptr);
    ocall_get_system_micros(&timerStop);
    timerPartitionUsec = timerStop - timerStart;
    timerStart = timerStop;

    auto joinresult = new result_t;
    if (config->MATERIALIZE) {
        joinresult->resultlist = new threadresult_t[nthreads]();
    }

    for (int i = 0; i < nthreads; ++i) {
        args[i].tid = i;
        args[i].crkJoin = crkJoin;
        if (config->MATERIALIZE) {
            args[i].threadresult = &(joinresult->resultlist[i]);
        }

        // if no cracking needed run simple, else run normal crkj
        if (config->CRACKING_THRESHOLD == 0) {
            rv = pthread_create(&threads[i], nullptr, runFunction, (void*)&args[i]);
        } else {
            rv = pthread_create(&threads[i], nullptr, run, (void*)&args[i]);
        }
        if (rv) {
            logger(ERROR, "pthread_create return value: %d", rv);
            ocall_throw("");
        }
    }

    for (int i = 0; i < nthreads; i++) {
        pthread_join(threads[i], nullptr);
    }
    ocall_stopTimer(&cycles);
    ocall_get_system_micros(&timerStop);
#ifdef PCM_COUNT
    ocall_get_system_counter_state("Join", 0);
#endif
    timerJoinUsec = timerStop - timerStart;

    join_result_t jr = crkJoin->getJoinResult();

    double throughput = (double) (relR->num_tuples + relS->num_tuples) / (double) (timerPartitionUsec + timerJoinUsec);
    joinresult->throughput = throughput;
    joinresult->totalresults = jr.matches;
    joinresult->nthreads = nthreads;

    logger(INFO, "CrkJ matches = %lu", jr.matches);
//    logger(INFO, "Timers - HT: %lu us, Tree: %lu us, Partition: %lu us, Probe: %lu us",
//           jr.timer_ht, jr.timer_tree, jr.timer_part, jr.timer_probe);
#ifdef TIMERS
    logger(INFO, "Phase build (cycles)     : %lu", jr.timer_ht);
    logger(INFO, "Phase partition (cycles) : %lu", jr.timer_part);
    logger(INFO, "Phase probe (cycles)     : %lu", jr.timer_probe);
    logger(INFO, "CPU Cycles-per-tuple     : %lu", cycles / (relR->num_tuples + relS->num_tuples));
    logger(INFO, "Time partition (usec)    : %lu (%.2lf%%)", timerPartitionUsec,
           (100.0 * (double)timerPartitionUsec / (double)(timerPartitionUsec + timerJoinUsec)));
    logger(INFO, "Time join (usec)         : %lu (%.2lf%%)", timerJoinUsec,
           (100.0 * (double)timerJoinUsec / (double)(timerPartitionUsec + timerJoinUsec)));
//    uint64_t joinTime = jr.join_partition_usec + jr.join_build_usec + jr.join_probe_usec;
    logger(INFO, "");
    uint64_t joinTime = jr.join_usec;
    logger(INFO, "Time join_partition      : %lu (%.2lf%%)", jr.join_partition_usec,
           (100.0 * (double)jr.join_partition_usec / (double)joinTime));
    logger(INFO, "Time join_build          : %lu (%.2lf%%)", jr.join_build_usec,
           (100.0 * (double)jr.join_build_usec / (double)joinTime));
    logger(INFO, "Time join_probe          : %lu (%.2lf%%)", jr.join_probe_usec,
           (100.0 * (double)jr.join_probe_usec / (double)joinTime));

#endif
    logger(INFO, "Time total (usec)        : %lu", (timerPartitionUsec + timerJoinUsec));
    logger(INFO, "Throughput (M rec/sec)   : %.4lf", throughput);
#ifdef COUNT_SCANNED_TUPLES
    logger(INFO, "Scanned Tuples           : %lu", jr.scanned_tuples);
    crkJoin->logBitMap();
#endif
#ifdef MEASURE_PARTITIONS
    std::string res;
    for (int k = 0; k < nthreads; k++) {
        for (int i  = 0; i < (1 << bits)/nthreads; i++) {
            uint64_t t = jr.timer_per_partition[k][i];
            res.append(std::to_string(t));
            if (i != ((1<<bits)/nthreads - 1)) {
                res.append(",");
            }
        }
        logger(INFO, "TimerPerPartitionThread%d : %s", k, res.c_str());
        res.clear();
    }
#endif
//    logger(INFO, "Millis per partition : ");
//    printf("MPP : ", nullptr);
//    for (int i = 0; i < (1<<config->RADIXBITS); i++) {
//        printf("%lu", jr.timer_per_partition[i]);
//        if (i < (1<<config->RADIXBITS) - 1) {
//            printf(",",nullptr);
//        } else {
//            printf("\n", nullptr);
//        }
//    }

    delete crkJoin;
    delete[] threads;
    delete[] args;

    return joinresult;
}

result_t * CRKJ(relation_t *relR, relation_t *relS, joinconfig_t * config)
{
    return CRKJ_template(relR, relS, config, partition_thread, run);
}

result_t * CRKJS(relation_t *relR, relation_t *relS, joinconfig_t * config)
{
    return CRKJ_template(relR, relS, config, partition_thread_simple, run_simple);
}

result_t * CRKJS_st(relation_t *relR, relation_t *relS, joinconfig_t * config)
{
    return CRKJ_st_template(relR, relS, config, &Join::join_simple_dfs_st, true);
}

result_t * CRKJF(relation_t *relR, relation_t *relS, joinconfig_t * config)
{
    config->CRACKING_THRESHOLD = 0;
    return CRKJ_template(relR, relS, config, partition_thread, run_fusion);
}

result_t * CRKJF_st(relation_t *relR, relation_t *relS, joinconfig_t * config)
{
    return CRKJ_st_template(relR, relS, config, &Join::joinFusion, false);;
}
