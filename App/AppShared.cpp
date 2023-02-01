/*
 * Copyright (C) 2011-2020 Intel Corporation. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *   * Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in
 *     the documentation and/or other materials provided with the
 *     distribution.
 *   * Neither the name of Intel Corporation nor the names of its
 *     contributors may be used to endorse or promote products derived
 *     from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

#include <string.h>
# include <pwd.h>
#include <sys/time.h>
#include <time.h>
#include <fstream>
#include <unistd.h>
#include <prj_params.h>

#include "sgx_urts.h"
#include "App.h"
#include "Lib/ErrorSupport.h"
#include "data-types.h"
#include "Logger.h"
#include "Lib/generator.h"
#include "commons.h"

#ifndef NATIVE_COMPILATION
#include "Enclave_u.h"
#endif

#ifdef PCM_COUNT
#include "pcm_commons.h"
#include "cpucounters.h"
#endif

#include "sgx_counters.h"

char experiment_filename [512];
int write_to_file = 0;

/* Initialize the enclave:
 *   Call sgx_create_enclave to initialize an enclave instance
 */
int initialize_enclave(sgx_enclave_id_t * enclaveId)
{
    sgx_status_t ret = SGX_ERROR_UNEXPECTED;
    
    /* Call sgx_create_enclave to initialize an enclave instance */
    /* Debug Support: set 2nd parameter to 1 */
    ret = sgx_create_enclave(ENCLAVE_FILENAME, SGX_DEBUG_FLAG, NULL, NULL, enclaveId, NULL);
    if (ret != SGX_SUCCESS) {
        ret_error_support(ret);
        return -1;
    }
    logger(INFO, "New enclave created  enclaveId = %d", *enclaveId);
#ifdef SGX_COUNTERS
    ocall_get_sgx_counters("Start enclave");
#endif
    return 0;
}

struct enclave_arg_t
{
    int tid;
    sgx_enclave_id_t enclaveId;
    relation_t * tableR;
    relation_t * tableS;
    char * algorithm;
    joinconfig_t * config;
    double * joinThroughput;
};

static void *
run (void * args)
{
    result_t results;
    enclave_arg_t * arg = reinterpret_cast<enclave_arg_t*>(args);
    logger(INFO, "[thread-%d] EnclaveId = %d", arg->tid, arg->enclaveId);
    sgx_status_t ret = ecall_join(arg->enclaveId,
                                  &results,
                                  arg->tableR,
                                  arg->tableS,
                                  arg->algorithm,
                                  arg->config);
    if (ret != SGX_SUCCESS) {
        logger(ERROR, "[thread-%d] join error", arg->tid);
        ret_error_support(ret);
    }
    arg->joinThroughput[arg->tid] = results.throughput;
    return nullptr;
}

/* Application entry */
int SGX_CDECL main(int argc, char *argv[])
{
    initLogger();
    struct table_t tableR;
    struct table_t tableS;
    result_t* results;
    sgx_status_t ret;
    struct timespec tw1, tw2;
    double time;
    joinconfig_t joinconfig;
    double* joinThroughput;

    logger(INFO, "************* MULTI_QUERY APP *************");

#ifdef PCM_COUNT
    initPCM();
#endif

    /* Cmd line parameters */
    args_t params;

    /* Set default values for cmd line params */
    set_default_args(&params);

    parse_args(argc, argv, &params, NULL);

    set_joinconfig(&joinconfig, &params);

    const int NUMBER_OF_QUERIES = params.number_of_queries;
    const int DELAY_MS = params.query_delay_ms;


    logger(INFO, "Delay between joins = %d ms", DELAY_MS);
    logger(DBG, "Number of threads = %d (N/A for every algorithm)", joinconfig.NTHREADS);

    relation_t * tablesR = new relation_t[NUMBER_OF_QUERIES];
    relation_t * tablesS = new relation_t[NUMBER_OF_QUERIES];
    sgx_enclave_id_t * enclaveIds = new sgx_enclave_id_t[NUMBER_OF_QUERIES];
    joinThroughput = new double[NUMBER_OF_QUERIES];

    seed_generator(params.r_seed);
    seed_generator(params.s_seed);

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

    logger(INFO, "Initialize %d enclaves", NUMBER_OF_QUERIES);
    for (int i = 0; i < NUMBER_OF_QUERIES; i++) {
        initialize_enclave(&enclaveIds[i]);
    }

    logger(INFO, "Running algorithm %s", params.algorithm_name);

    clock_gettime(CLOCK_MONOTONIC, &tw1); // POSIX; use timespec_get in C11

    pthread_t * threads = new pthread_t[NUMBER_OF_QUERIES];
    enclave_arg_t * args = new enclave_arg_t[NUMBER_OF_QUERIES];

    for (int i = 0; i < NUMBER_OF_QUERIES; i++) {
        args[i].tid = i;
        args[i].enclaveId = enclaveIds[i];
        args[i].config = &joinconfig;
        args[i].algorithm = params.algorithm_name;
        args[i].tableR = &tablesR[i];
        args[i].tableS = &tablesS[i];
        args[i].joinThroughput = joinThroughput;

        int rv = pthread_create(&threads[i], NULL, run, (void*)&args[i]);
        if (rv) {
            logger(ERROR, "Thread-%d return value: %d", i, rv);
            ocall_throw("");
        }
        usleep(DELAY_MS * 1000);
    }

    for (int i = 0; i < NUMBER_OF_QUERIES; i++) {
        pthread_join(threads[i], NULL);
    }

    clock_gettime(CLOCK_MONOTONIC, &tw2);
    time = 1000.0*(double)tw2.tv_sec + 1e-6*(double)tw2.tv_nsec
                        - (1000.0*(double)tw1.tv_sec + 1e-6*(double)tw1.tv_nsec);
    logger(INFO, "Total join runtime: %.2fs", time/1000);

    double avgThroughput = 0;
    for (int i = 0; i < NUMBER_OF_QUERIES; i++) {
        logger(INFO, "Q%d Throughput = %.4lf (M rec/sec", i, joinThroughput[i]);
        avgThroughput += joinThroughput[i];
    }
    avgThroughput /= NUMBER_OF_QUERIES;
    logger(INFO, "Average throughput = %.4lf (M rec/sec)", avgThroughput);


    for (int i = 0; i < NUMBER_OF_QUERIES; i++) {
        sgx_destroy_enclave(enclaveIds[i]);
        delete_relation(&tablesR[i]);
        delete_relation(&tablesS[i]);
    }

    delete[] tablesR;
    delete[] tablesS;
    delete[] enclaveIds;
    delete[] threads;
    delete[] args;
    delete[] joinThroughput;

    return 0;
}
