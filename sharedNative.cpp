#include <math.h>
#include <time.h>

#include "data-types.h"
#include "commons.h"
#include "generator.h"
#include "Logger.h"

#include "RadixJoin/radix_join.h"
#include "CrkJoin/JoinWrapper.hpp"
#include "pameco/PMCJoin.h"
#include "pameco/CMemoryManagement.h"
#include "mcjoin/MCJoin.h"
#include "BlockNestedHashJoin/no_partitioning_join.h"

#include "pcm_commons.h"
#include "prj_params.h"

#ifdef PCM_COUNT
#include "cpucounters.h"
#endif

using namespace std;

static result_t* PMCJ(struct table_t * relR, struct table_t * relS, joinconfig_t * config);
static result_t* MCJoinInit(struct table_t * relR, struct table_t * relS, joinconfig_t * config);

static struct algorithm_t algorithms[] = {
        {"RJ", RJ},
        {"RHO", RHO},
        {"RHT", RHT},
        {"CRKJ", CRKJ},
        {"CrkJoin", CRKJ},
        {"PaMeCo", PMCJ},
        {"MCJoin", MCJoinInit},
        {"BHJ",BHJ},
};

struct shared_native_args_t
{
    int tid;
    relation_t * tableR;
    relation_t * tableS;
    algorithm_t * algorithm;
    joinconfig_t * config;
    double * joinThroughput;
};

static void *
run (void * args)
{
    auto * arg = reinterpret_cast<shared_native_args_t*>(args);
    logger(DBG, "[thread-%d] Start joining", arg->tid);
    result_t* res = arg->algorithm->join(arg->tableR, arg->tableS, arg->config);
    logger(DBG, "[thread-%d] Done joining", arg->tid);
    arg->joinThroughput[arg->tid] = res->throughput;
    return nullptr;
}

static result_t*
MCJoinInit(struct table_t * relR, struct table_t * relS, joinconfig_t * config)
{
    auto * joinresult = (result_t*) malloc(sizeof(result_t));
    joinresult->resultlist = (threadresult_t *) malloc(sizeof(threadresult_t));

    MCJ::SMetrics metrics;
    uint32_t memoryConstraintMB = config->MB_RESTRICTION;
    uint32_t packedPartitionMemoryCardinality = 16000000;
    uint32_t flipFlopCardinality = 16000000;
    uint32_t bitRadixLength = 24;
    uint32_t maxBitsPerFlipFlopPass = 8;
    uint32_t outputBufferCardinality = 1000;
    // parse arguments
    MCJ::CMemoryManagement memManagement;
    logger(INFO, "Memory constraint %d MB", memoryConstraintMB);

    if (memoryConstraintMB != 0)
    {
        if(memManagement.optimiseForMemoryConstraint((uint32_t)relR->num_tuples, memoryConstraintMB, bitRadixLength))
        {
            // Based on the results, we need to change our options settings
            packedPartitionMemoryCardinality = memManagement.getRIdealTuples();
            flipFlopCardinality = memManagement.getSIdealTuples();
            metrics.r_bias = memManagement.getRBias();
        }
        else
        {
            // It didn't work - this is usually because the histogram is simply too large to fit within the given memory constraint
            logger(ERROR, "Unable to allocate memory (histogram too large for constraint?)");
            ocall_exit(-1);
        }
    }

    MCJ::CMemoryManagement memoryManagement;


    MCJoin mcj = MCJoin();
    mcj.doMCJoin(relR,
                 relS,
                 &metrics,
                 bitRadixLength,
                 maxBitsPerFlipFlopPass,
                 config->NTHREADS,
                 flipFlopCardinality,
                 packedPartitionMemoryCardinality,
                 outputBufferCardinality,
                 config->MATERIALIZE,
                 &(joinresult->resultlist[0]));
    joinresult->throughput = (double)(relR->num_tuples + relS->num_tuples) / (double) metrics.time_total_us;
    joinresult->nthreads = 1;
    joinresult->totalresults = metrics.output_cardinality;
    return joinresult;
}

static result_t*
PMCJ(struct table_t * relR, struct table_t * relS, joinconfig_t * config)
{
    auto * joinresult = (result_t*) malloc(sizeof(result_t));    PMC::SMetrics metrics = PMC::SMetrics();
    metrics.r_cardinality = relR->num_tuples;
    metrics.s_cardinality = relS->num_tuples;

    PMC::COptions options;

    options.setTechLevel(2);
    options.setThreads(static_cast<uint32_t>(config->NTHREADS));
    options.setBitRadixLength(24);
    options.setMaxBitsPerFlipFlopPass(8);
    options.setMemoryConstraintMB(config->MB_RESTRICTION);
    options.setCompactHashTableCardinality(0);
    options.setFlipFlopCardinality(0);
    options.setOutputBufferCardinality(100000);
    options.setRelationR(relR);
    options.setRelationS(relS);
    if(options.getMemoryConstraintMB() != 0)
    {
        PMC::CMemoryManagement memManagement;
        if(options.getBias() == 0.0) {
            if (memManagement.optimiseForMemoryConstraint(&options)) {
                options.setCompactHashTableCardinality(memManagement.getRIdealTuples());
                options.setFlipFlopCardinality(memManagement.getSIdealTuples());
                metrics.r_bias = memManagement.getRBias();
            } else {
                logger(ERROR, "Unable to allocate memory (histogram too large for constraint?)");
                ocall_exit(-1);
            }
        }
        else
        {
            if (memManagement.allocateMemoryForRBias(options.getBias(), &options))
            {
                // Based on the results, we need to change our options settings
                options.setCompactHashTableCardinality(memManagement.getRIdealTuples());
                options.setFlipFlopCardinality(memManagement.getSIdealTuples());
                metrics.r_bias = memManagement.getRBias();
            }
            else
            {
                logger(ERROR, "Unable to allocate memory (histogram too large for constraint?)");
                ocall_exit(-1);
            }
        }
    }
    else
    {
        // Set number of spinlocks for no constraint
        if (options.getThreads() > 1)
        {
            options.setSpinlocks(options.getCompactHashTableCardinality() / options.getSpinlockDivisor());
        }
    }

    PMC::PMCJoin pmcj = PMC::PMCJoin(&options, &metrics);
    PMC::Timer tmrJoin;

    tmrJoin.update();
    pmcj.doPMCJoin();
    tmrJoin.update();

    metrics.time_total_us = tmrJoin.getElapsedMicros();
    metrics.logMetrics();

    joinresult->throughput = (double)(relR->num_tuples + relS->num_tuples) / (double) metrics.time_total_us;

    return joinresult;
}

int main(int argc, char *argv[]) {
    initLogger();
    logger(INFO, "Welcome from native!");

#ifdef PCM_COUNT
    initPCM();
#endif

    /* Cmd line parameters */
    args_t params;

    /* Set default values for cmd line params */
    set_default_args(&params);
    params.algorithm = &algorithms[1];

    parse_args(argc, argv, &params, algorithms);

    joinconfig_t joinconfig;
    set_joinconfig(&joinconfig, &params);
    joinconfig.PRINT = 1;
    const int NUMBER_OF_QUERIES = params.number_of_queries;
    const int DELAY_MS = params.query_delay_ms;

    logger(INFO, "Delay between joins = %d ms", DELAY_MS);
    logger(DBG, "Number of threads = %d (N/A for every algorithm)", joinconfig.NTHREADS);

    seed_generator(params.r_seed);
    seed_generator(params.s_seed);

    relation_t * tablesR = new relation_t[NUMBER_OF_QUERIES];
    relation_t * tablesS = new relation_t[NUMBER_OF_QUERIES];
    double* joinThroughput = new double[NUMBER_OF_QUERIES];

    logger(INFO, "Generate data for %d queries", NUMBER_OF_QUERIES);
    if (params.r_from_path) {
        logger(INFO, "Build relation R from file %s", params.r_path);
        for (int i = 0; i < NUMBER_OF_QUERIES; i++) {
            create_relation_from_file(&tablesR[i], params.r_path, params.sort_r);
        }
        params.r_size = tablesR[0].num_tuples;
        logger(INFO, "R size = %.2lf MB (%u tuples)",
               B_TO_MB(sizeof(struct row_t) * (double) params.r_size),
               params.r_size);
    } else {
        logger(INFO, "Build relation R with size = %.2lf MB (%u tuples)",
               B_TO_MB(sizeof(struct row_t) * params.r_size),
               params.r_size);
        for (int i = 0; i < NUMBER_OF_QUERIES; i++) {
            create_relation_pk(&tablesR[i], params.r_size, params.sort_r);
        }
    }
    logger(DBG, "DONE");

    if (params.s_from_path) {
        logger(INFO, "Build relation S from file %s", params.s_path);
        for (int i = 0; i < NUMBER_OF_QUERIES; i++) {
            create_relation_from_file(&tablesS[i], params.s_path, params.sort_s);
        }
        params.s_size = tablesS[0].num_tuples;
        logger(INFO, "S size = %.2lf MB (%u tuples)",
               B_TO_MB(sizeof(struct row_t) * (double) params.s_size),
               params.s_size);
    } else {
        logger(INFO, "Build relation S with size = %.2lf MB (%u tuples)",
               B_TO_MB(sizeof(struct row_t) * params.s_size),
               params.s_size);
        for (int i = 0; i < NUMBER_OF_QUERIES; i++) {
            create_relation_fk(&tablesS[i], params.s_size, params.r_size, params.sort_s);
        }
    }
    logger(DBG, "DONE");

    logger(INFO, "Running algorithm %s", params.algorithm->name);
    pthread_t * threads = new pthread_t[NUMBER_OF_QUERIES];
    auto * args = new shared_native_args_t[NUMBER_OF_QUERIES];

    struct rusage_reduced_t usage;
    usage.ru_utime_sec = 0;
    usage.ru_utime_usec = 0;
    usage.ru_stime_sec = 0;
    usage.ru_stime_usec = 0;
    usage.ru_minflt = 0;
    usage.ru_majflt = 0;
    usage.ru_nvcsw = 0;
    usage.ru_nivcsw = 0;
    ocall_getrusage(&usage, 0);
#ifdef PCM_COUNT
    ocall_set_system_counter_state("Start ecall join");
#endif
    clock_t tictoc = clock();
    for (int i = 0; i < NUMBER_OF_QUERIES; i++) {
        args[i].tid = i;
        args[i].config = &joinconfig;
        args[i].algorithm = params.algorithm;
        args[i].tableR = &tablesR[i];
        args[i].tableS = &tablesS[i];
        args[i].joinThroughput = joinThroughput;
        int rv = pthread_create(&threads[i], nullptr, run, (void*)&args[i]);
        if (rv) {
            logger(ERROR, "Thread-%d return value: %d", i, rv);
            ocall_throw("");
        }
    }
//    result_t* matches = params.algorithm->join(&tableR, &tableS, &joinconfig);
    for (int i = 0; i < NUMBER_OF_QUERIES; i++) {
        pthread_join(threads[i], nullptr);
    }
    tictoc = clock() - tictoc;
    ocall_getrusage(&usage, 1);
#ifdef PCM_COUNT
    ocall_get_system_counter_state("End ecall join", 1);
#endif
    logger(INFO, "Total join runtime: %.2fs", (tictoc)/ (float)(CLOCKS_PER_SEC));
    double avgThroughput = 0;
    for (int i = 0; i < NUMBER_OF_QUERIES; i++) {
        logger(INFO, "Q%d Throughput = %.4lf (M rec/sec", i, joinThroughput[i]);
        avgThroughput += joinThroughput[i];
    }
    avgThroughput /= NUMBER_OF_QUERIES;
    logger(INFO, "Average throughput = %.4lf (M rec/sec)", avgThroughput);


    for (int i = 0; i < NUMBER_OF_QUERIES; i++) {
        delete_relation(&tablesR[i]);
        delete_relation(&tablesS[i]);
    }
    delete[] tablesR;
    delete[] tablesS;
    delete[] threads;
    delete[] args;
    delete[] joinThroughput;

}