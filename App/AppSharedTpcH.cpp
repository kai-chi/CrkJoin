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
#include <TpcHCommons.hpp>

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

struct enclave_tpch_arg_t
{
    int tid;
    sgx_enclave_id_t enclaveId;
    char * algorithm;
    joinconfig_t * config;
    double * joinThroughput;
    LineItemTable *l;
    OrdersTable *o;
    CustomerTable *c;
    PartTable *p;
    NationTable *n;
    uint8_t query;
};

static void *
run (void * args)
{
    result_t results;
    sgx_status_t retval = SGX_ERROR_UNEXPECTED, ret = SGX_ERROR_UNEXPECTED;
    auto * arg = reinterpret_cast<enclave_tpch_arg_t*>(args);
    logger(INFO, "[thread-%d] EnclaveId = %d", arg->tid, arg->enclaveId);

    switch(arg->query) {
        case 3:
            ret = ecall_tpch_q3(arg->enclaveId,
                                &retval,
                                &results,
                                arg->c,
                                arg->o,
                                arg->l,
                                arg->algorithm,
                                arg->config);
            break;
        case 10:
            ret = ecall_tpch_q10(arg->enclaveId,
                                 &retval,
                                 &results,
                                 arg->c,
                                 arg->o,
                                 arg->l,
                                 arg->n,
                                 arg->algorithm,
                                 arg->config);
            break;
        case 12:
            ret = ecall_tpch_q12(arg->enclaveId,
                                 &retval,
                                 &results,
                                 arg->l,
                                 arg->o,
                                 arg->algorithm,
                                 arg->config);
            break;
        case 19:
            ret = ecall_tpch_q19(arg->enclaveId,
                                 &retval,
                                 &results,
                                 arg->l,
                                 arg->p,
                                 arg->algorithm,
                                 arg->config);
            break;
        default:
            logger(ERROR, "TPC-H Q%d is not supported", arg->query);
    }

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
    result_t* results;
    sgx_status_t ret;
    struct timespec tw1, tw2;
    double time;
    joinconfig_t joinconfig;
    double* joinThroughput;

    uint8_t queries[8] = {3, 10, 12, 19, 3, 10, 12, 19};
    uint8_t SCALE = 10;

    logger(INFO, "************* CONCURRENT TPCH APP *************");

#ifdef PCM_COUNT
    initPCM();
#endif

    /* Cmd line parameters */
    args_t params;

    /* Set default values for cmd line params */
//    params.algorithm    = &algorithms[0]; /* NPO_st */
    params.r_size            = 2097152; /* 2*2^20 */
    params.s_size            = 2097152; /* 2*2^20 */
    params.r_seed            = 11111;
    params.s_seed            = 22222;
    params.nthreads          = 2;
    params.selectivity       = 100;
    params.skew              = 0;
    params.seal_chunk_size   = 0;
    params.seal              = 0;
    params.sort_r            = 0;
    params.sort_s            = 0;
    params.r_from_path       = 0;
    params.s_from_path       = 0;
    params.three_way_join    = 0;
    params.bits              = 13;
    params.write_to_file     = 0;
    params.mb_restriction    = 0;
    params.number_of_queries = 8;
    params.query_delay_ms    = 0;
    strcpy(params.algorithm_name, "CrkJoin");

    parse_args(argc, argv, &params, NULL);

    joinconfig.NTHREADS = (int) params.nthreads;
    joinconfig.RADIXBITS = params.bits;
    joinconfig.WRITETOFILE = params.write_to_file;
    joinconfig.MB_RESTRICTION = params.mb_restriction;
    joinconfig.MATERIALIZE = false;

    const int NUMBER_OF_QUERIES = params.number_of_queries;
    const int DELAY_MS = params.query_delay_ms;


    logger(INFO, "Delay between joins = %d ms", DELAY_MS);
    logger(DBG, "Number of threads = %d (N/A for every algorithm)", joinconfig.NTHREADS);

//    relation_t * tablesR = new relation_t[NUMBER_OF_QUERIES];
//    relation_t * tablesS = new relation_t[NUMBER_OF_QUERIES];
    auto *l = new LineItemTable[NUMBER_OF_QUERIES];
    auto *o = new OrdersTable[NUMBER_OF_QUERIES];
    auto *c = new CustomerTable[NUMBER_OF_QUERIES];
    auto *p = new PartTable[NUMBER_OF_QUERIES];
    auto *n = new NationTable[NUMBER_OF_QUERIES];

    for (int i = 0; i < NUMBER_OF_QUERIES; i++) {
        load_orders_from_file(&o[i], queries[i], SCALE);
        load_customer_from_file(&c[i], queries[i], SCALE);
        load_part_from_file(&p[i], queries[i], SCALE);
        load_nation_from_file(&n[i], queries[i], SCALE);
        load_lineitem_from_file(&l[i], queries[i], SCALE);
    }

    auto * enclaveIds = new sgx_enclave_id_t[NUMBER_OF_QUERIES];
    joinThroughput = new double[NUMBER_OF_QUERIES];

    logger(INFO, "Initialize %d enclaves", NUMBER_OF_QUERIES);
    for (int i = 0; i < NUMBER_OF_QUERIES; i++) {
        initialize_enclave(&enclaveIds[i]);
    }

    logger(INFO, "Running algorithm %s", params.algorithm_name);

    clock_gettime(CLOCK_MONOTONIC, &tw1); // POSIX; use timespec_get in C11

    auto * threads = new pthread_t[NUMBER_OF_QUERIES];
    auto * args = new enclave_tpch_arg_t[NUMBER_OF_QUERIES];

    for (int i = 0; i < NUMBER_OF_QUERIES; i++) {
        args[i].tid = i;
        args[i].enclaveId = enclaveIds[i];
        args[i].config = &joinconfig;
        args[i].algorithm = params.algorithm_name;
        args[i].l = &l[i];
        args[i].o = &o[i];
        args[i].c = &c[i];
        args[i].p = &p[i];
        args[i].n = &n[i];
        args[i].joinThroughput = joinThroughput;
        args[i].query = queries[i];

        int rv = pthread_create(&threads[i], NULL, run, (void*)&args[i]);
        if (rv) {
            logger(ERROR, "Thread-%d return value: %d", i, rv);
            ocall_throw("");
        }
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
        free_orders(&o[i], queries[i]);
        free_part(&p[i], queries[i]);
        free_customer(&c[i], queries[i]);
        free_lineitem(&l[i], queries[i]);
        free_nation(&n[i], queries[i]);
    }

    delete[] enclaveIds;
    delete[] threads;
    delete[] args;
    delete[] joinThroughput;

    return 0;
}
