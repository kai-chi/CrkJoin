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
#include "NestedLoop/nested_loop_join.h"
#include "mcjoin/MCJoin.h"

#include "pcm_commons.h"

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
        {"NL", NL},
        {"MCJoin", MCJoinInit}
};

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
    result_t * joinresult = nullptr;
    PMC::SMetrics metrics = PMC::SMetrics();
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

    return joinresult;
}

int main(int argc, char *argv[]) {
    initLogger();
    logger(INFO, "Welcome from native!");

#ifdef PCM_COUNT
    initPCM();
#endif

    struct table_t tableR;
    struct table_t tableS;
    int64_t results;

    /* Cmd line parameters */
    args_t params;

    /* Set default values for cmd line params */
    params.algorithm       = &algorithms[0]; /* NPO_st */
    params.r_size          = 2097152; /* 2*2^20 */
    params.s_size          = 2097152; /* 2*2^20 */
    params.r_seed          = 11111;
    params.s_seed          = 22222;
    params.nthreads        = 2;
    params.selectivity     = 100;
    params.skew            = 0;
    params.sort_r          = 0;
    params.sort_s          = 0;
    params.r_from_path     = 0;
    params.s_from_path     = 0;
    params.seal_chunk_size = 0;
    params.three_way_join  = 0;
    params.bits            = 13;
    params.mb_restriction  = 0;
    params.write_to_file   = 0;

    parse_args(argc, argv, &params, algorithms);

    joinconfig_t joinconfig;
    joinconfig.NTHREADS = (int) params.nthreads;
    joinconfig.RADIXBITS = params.bits;
    joinconfig.WRITETOFILE = params.write_to_file;
    joinconfig.MATERIALIZE = false;
    joinconfig.MB_RESTRICTION = params.mb_restriction;

    logger(DBG, "Number of threads = %d (N/A for every algorithm)", params.nthreads);

    seed_generator(params.r_seed);
    if (params.r_from_path)
    {
        logger(INFO, "Build relation R from file %s", params.r_path);
        create_relation_from_file(&tableR, params.r_path, params.sort_r);
        params.r_size = tableR.num_tuples;
    }
    else
    {
        logger(INFO, "Build relation R with size = %.2lf MB (%d tuples)",
               (double) sizeof(struct row_t) * params.r_size/pow(2,20),
               params.r_size);
        create_relation_pk(&tableR, params.r_size, params.sort_r);
    }
    logger(DBG, "DONE");

    seed_generator(params.s_seed);
    if (params.s_from_path)
    {
        logger(INFO, "Build relation S from file %s", params.s_path);
        create_relation_from_file(&tableS, params.s_path, params.sort_s);
        params.s_size = tableS.num_tuples;
    }
    else
    {
        logger(INFO, "Build relation S with size = %.2lf MB (%d tuples)",
               (double) sizeof(struct row_t) * params.s_size/pow(2,20),
               params.s_size);
        if (params.skew > 0) {
            create_relation_zipf(&tableS, params.s_size, params.r_size, params.skew, params.sort_s);
        }
        else if (params.selectivity != 100)
        {
            logger(INFO, "Table S selectivity = %d", params.selectivity);
            uint32_t maxid = params.selectivity != 0 ? (100 * params.r_size / params.selectivity) : 0;
            create_relation_fk_sel(&tableS, params.s_size, maxid, params.sort_s);
        }
        else {
            create_relation_fk(&tableS, params.s_size, params.r_size, params.sort_s);
        }
    }
    logger(DBG, "DONE");

    logger(INFO, "Running algorithm %s", params.algorithm->name);
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
    result_t* matches = params.algorithm->join(&tableR, &tableS, &joinconfig);
    tictoc = clock() - tictoc;
    ocall_getrusage(&usage, 1);
#ifdef PCM_COUNT
    ocall_get_system_counter_state("End ecall join", 1);
#endif
    logger(INFO, "Total join runtime: %.2fs", (tictoc)/ (float)(CLOCKS_PER_SEC));
    logger(INFO, "Matches = %lu", matches->totalresults);
    delete_relation(&tableR);
    delete_relation(&tableS);
}