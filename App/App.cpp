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

/* Global EID shared by multiple threads */
sgx_enclave_id_t global_eid = 0;

char experiment_filename [512];
int write_to_file = 0;

/* Initialize the enclave:
 *   Call sgx_create_enclave to initialize an enclave instance
 */
int initialize_enclave(void)
{
    sgx_status_t ret = SGX_ERROR_UNEXPECTED;

    logger(INFO, "Initialize enclave");
    /* Call sgx_create_enclave to initialize an enclave instance */
    /* Debug Support: set 2nd parameter to 1 */
    ret = sgx_create_enclave(ENCLAVE_FILENAME, SGX_DEBUG_FLAG, NULL, NULL, &global_eid, NULL);
    if (ret != SGX_SUCCESS) {
        ret_error_support(ret);
        return -1;
    }
    logger(INFO, "Enclave id = %d", global_eid);
#ifdef SGX_COUNTERS
    ocall_get_sgx_counters("Start enclave");
#endif
    return 0;
}

uint8_t* seal_relation(struct table_t * rel, uint32_t* size)
{
    sgx_status_t ret = ecall_get_sealed_data_size(global_eid, size, rel);
    if (ret != SGX_SUCCESS )
    {
        ret_error_support(ret);
        return nullptr;
    }
    else if (*size == UINT32_MAX)
    {
        logger(ERROR, "get_sealed_data_size failed");
        return nullptr;
    }

    logger(DBG, "Allocate for sealed relation %.2lf MB", B_TO_MB(*size));
    uint8_t* sealed_rel_tmp = (uint8_t *) malloc(*size);
    if (sealed_rel_tmp == nullptr)
    {
        logger(ERROR, "Out of memory");
        return nullptr;
    }
    sgx_status_t retval;
    ret = ecall_seal_data(global_eid, &retval, rel, sealed_rel_tmp, *size);
    if (ret != SGX_SUCCESS)
    {
        ret_error_support(ret);
        free(sealed_rel_tmp);
        return nullptr;
    }
    else if (retval != SGX_SUCCESS)
    {
        ret_error_support(retval);
        free(sealed_rel_tmp);
        return nullptr;
    }
    logger(DBG, "Sealing successful");
    return sealed_rel_tmp;
}

/* Application entry */
int SGX_CDECL main(int argc, char *argv[])
{
    initLogger();
    struct table_t tableR;
    struct table_t tableS;
    result_t results{};
    uint8_t * sealed_result = nullptr;
    sgx_status_t ret;
    struct timespec tw1, tw2;
    double time;
    uint8_t  *sealed_R = nullptr, *sealed_S = nullptr;
    uint32_t sealed_data_size_r = 0, sealed_data_size_s = 0;
    joinconfig_t joinconfig;

    logger(INFO, "************* TEE_BENCH APP *************");

#ifdef PCM_COUNT
    initPCM();
#endif

    /* Cmd line parameters */
    args_t params;

    /* Set default values for cmd line params */
    set_default_args(&params);

    parse_args(argc, argv, &params, NULL);

    joinconfig.NTHREADS = (int) params.nthreads;
    joinconfig.RADIXBITS = params.bits;
    joinconfig.WRITETOFILE = params.write_to_file;
    joinconfig.MB_RESTRICTION = params.mb_restriction;
    joinconfig.MATERIALIZE = false;
    joinconfig.PRINT = 1;
    joinconfig.CRACKING_THRESHOLD = params.crackingThreshold;

    if (joinconfig.WRITETOFILE) {
        sprintf(experiment_filename, "%s-%s-%lu",
                params.experiment_name, params.algorithm_name, ocall_get_system_micros()/100000);
        write_to_file = 1;
        logger(DBG, "Experiment filename: %s", experiment_filename);
    }

    logger(DBG, "Number of threads = %d (N/A for every algorithm)", joinconfig.NTHREADS);

#ifdef ITT_NOTIFS
    initialize_itt_events();
    ocall_itt_event_start(0);
#endif

    seed_generator(params.r_seed);
    // This is just a hacky way to run the three-way join experiment. This should not be in this file forever.
    if (params.three_way_join) {
        struct table_t tableT;
        uint8_t *sealed_T = nullptr;
        uint32_t sealed_data_size_t = 0;
        logger(INFO, "Running the three-way join experiment");
        logger(INFO, "Build relation R with size = %.2lf MB (%u tuples)",
               B_TO_MB(sizeof(struct row_t) * (double) params.r_size),
               params.r_size);
        create_relation_pk(&tableR, params.r_size, params.sort_r);
        logger(DBG, "DONE");
        seed_generator(params.s_seed);
        logger(INFO, "Build relation S with size = %.2lf MB (%u tuples)",
               B_TO_MB(sizeof(struct row_t) * (double) params.r_size),
               params.r_size);
        logger(INFO, "Build relation T with size = %.2lf MB (%u tuples)",
               B_TO_MB(sizeof(struct row_t) * (double) params.s_size),
               params.s_size);
        if (params.selectivity != 100) {
            logger(INFO, "Table S selectivity = %d", params.selectivity);
            uint32_t maxid = (uint32_t) (params.selectivity != 0 ? (100 * params.r_size / params.selectivity) : 0);
            create_relation_fk_sel(&tableS, params.r_size, maxid, params.sort_s);
            logger(DBG, "DONE");
            create_relation_fk_sel(&tableT, params.s_size, maxid, params.sort_s);
            logger(DBG, "DONE");
        }
        else {
            create_relation_fk(&tableS, params.r_size, params.r_size, params.sort_s);
            logger(DBG, "DONE");
            create_relation_fk(&tableT, params.s_size, params.r_size, params.sort_s);
            logger(DBG, "DONE");
        }

        initialize_enclave();
        sealed_R = seal_relation(&tableR, &sealed_data_size_r);
        if (sealed_R == nullptr)
        {
            sgx_destroy_enclave(global_eid);
            delete_relation(&tableR);
            delete_relation(&tableS);
            delete_relation(&tableT);
            exit(EXIT_FAILURE);
        }
        sealed_S = seal_relation(&tableS, &sealed_data_size_s);
        if (sealed_S == nullptr)
        {
            sgx_destroy_enclave(global_eid);
            delete_relation(&tableR);
            delete_relation(&tableS);
            delete_relation(&tableT);
            exit(EXIT_FAILURE);
        }
        sealed_T = seal_relation(&tableT, &sealed_data_size_t);
        if (sealed_T == nullptr)
        {
            sgx_destroy_enclave(global_eid);
            delete_relation(&tableR);
            delete_relation(&tableS);
            delete_relation(&tableT);
            exit(EXIT_FAILURE);
        }
        logger(INFO, "Running algorithm %s", params.algorithm_name);
        uint32_t  sealed_data_size = 0;

        ret = ecall_three_way_join_sealed_tables(global_eid,
                                                 &sealed_data_size,
                                                 sealed_R,
                                                 sealed_data_size_r,
                                                 sealed_S,
                                                 sealed_data_size_s,
                                                 sealed_T,
                                                 sealed_data_size_t,
                                                 params.algorithm_name,
                                                 &joinconfig,
                                                 params.seal_chunk_size);
        if (ret != SGX_SUCCESS)
        {
            ret_error_support(ret);
        }
        logger(DBG, "Sealed data size = %.2lf MB", B_TO_MB(sealed_data_size));
        sealed_result = (uint8_t*)malloc(sealed_data_size);
        if (sealed_result == nullptr)
        {
            logger(ERROR, "Out of memory");
            exit(EXIT_FAILURE);
        }
        sgx_status_t retval;
        ret = ecall_get_sealed_data(global_eid, &retval, sealed_result, sealed_data_size);
        if (ret != SGX_SUCCESS)
        {
            ret_error_support(ret);
        }
        sgx_destroy_enclave(global_eid);
        delete_relation(&tableR);
        delete_relation(&tableS);
        delete_relation(&tableT);
        free(sealed_R);
        free(sealed_S);
        free(sealed_T);
        free(sealed_result);
        // End three-way experiment
        exit(EXIT_SUCCESS);
    }

    if (params.r_from_path)
    {
        logger(INFO, "Build relation R from file %s", params.r_path);
        create_relation_from_file(&tableR, params.r_path, params.sort_r);
        params.r_size = tableR.num_tuples;
        logger(INFO, "R size = %.2lf MB (%u tuples)",
               B_TO_MB(sizeof(struct row_t) * (double) params.r_size),
               params.r_size);
    }
    else
    {
        logger(INFO, "Build relation R with size = %.2lf MB (%u tuples)",
               B_TO_MB(sizeof(struct row_t) * (double) params.r_size),
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
        logger(INFO, "S size = %.2lf MB (%u tuples)",
               B_TO_MB(sizeof(struct row_t) * (double) params.s_size),
               params.s_size);
    }
    else
    {
        logger(INFO, "Build relation S with size = %.2lf MB (%u tuples)",
               B_TO_MB(sizeof(struct row_t) * (double) params.s_size),
               params.s_size);
        if (params.skew > 0)
        {
            logger(INFO, "Skew relation: %.2lf", params.skew);
            create_relation_zipf(&tableS, params.s_size, params.r_size, params.skew, params.sort_s);
        }
        else if (params.selectivity != 100)
        {
            logger(INFO, "Table S selectivity = %d", params.selectivity);
            uint32_t maxid = (uint32_t) (params.selectivity != 0 ? (100 * params.r_size / params.selectivity) : 0);
            create_relation_fk_sel(&tableS, params.s_size, maxid, params.sort_s);
        }
        else {
            create_relation_fk(&tableS, params.s_size, params.r_size, params.sort_s);
        }
    }

    logger(DBG, "DONE");

    initialize_enclave();

    if (params.seal)
    {
        sealed_R = seal_relation(&tableR, &sealed_data_size_r);
        if (sealed_R == nullptr)
        {
            sgx_destroy_enclave(global_eid);
            delete_relation(&tableR);
            delete_relation(&tableS);
            exit(EXIT_FAILURE);
        }
        sealed_S = seal_relation(&tableS, &sealed_data_size_s);
        if (sealed_S == nullptr)
        {
            sgx_destroy_enclave(global_eid);
            delete_relation(&tableR);
            delete_relation(&tableS);
            exit(EXIT_FAILURE);
        }
    }

    logger(INFO, "Running algorithm %s", params.algorithm_name);

    clock_gettime(CLOCK_MONOTONIC, &tw1); // POSIX; use timespec_get in C11
    if (params.seal)
    {
        uint64_t total_cycles, retrieve_data_timer;
        uint32_t sealed_data_size = 0;
        ocall_startTimer(&total_cycles);
        ret = ecall_join_sealed_tables(global_eid,
                                       &sealed_data_size,
                                       sealed_R,
                                       sealed_data_size_r,
                                       sealed_S,
                                       sealed_data_size_s,
                                       params.algorithm_name,
                                       &joinconfig,
                                       params.seal_chunk_size);
        if (ret != SGX_SUCCESS)
        {
            ret_error_support(ret);
        }
        logger(DBG, "Sealed data size = %.2lf MB", B_TO_MB(sealed_data_size));
        sealed_result = (uint8_t*)malloc(sealed_data_size);
        if (sealed_result == nullptr)
        {
            logger(ERROR, "Out of memory");
            exit(EXIT_FAILURE);
        }
        sgx_status_t retval;
        ocall_startTimer(&retrieve_data_timer);
        ret = ecall_get_sealed_data(global_eid, &retval, sealed_result, sealed_data_size);
        ocall_stopTimer(&retrieve_data_timer);
        if (ret != SGX_SUCCESS)
        {
            ret_error_support(ret);
        }
        ocall_stopTimer(&total_cycles);
        logger(INFO, "retrieve_data_timer = %lu", retrieve_data_timer);
        logger(INFO, "total_cycles        = %lu", total_cycles);
    }
    else {
#ifdef SGX_COUNTERS
        uint64_t ewb = ocall_get_ewb();
#endif
#ifdef PCM_COUNT
        ocall_set_system_counter_state("Start ecall join");
#endif
        ret = ecall_join(global_eid,
                         &results,
                         &tableR,
                         &tableS,
                         params.algorithm_name,
                         &joinconfig);
#ifdef PCM_COUNT
        ocall_get_system_counter_state("End ecall join", 1);
#endif
#ifdef SGX_COUNTERS
        ewb = ocall_get_ewb();
        logger(DBG, "ewb after join = %lu", ewb);
#endif
    }
    clock_gettime(CLOCK_MONOTONIC, &tw2);
    time = 1000.0*(double)tw2.tv_sec + 1e-6*(double)tw2.tv_nsec
                        - (1000.0*(double)tw1.tv_sec + 1e-6*(double)tw1.tv_nsec);
    logger(INFO, "Total join runtime: %.2fs", time/1000);
    logger(INFO, "throughput = %.4lf [M rec / s]",
           (double)(params.r_size + params.s_size)/(1000*time));
    if (ret != SGX_SUCCESS) {
        ret_error_support(ret);
    }

#ifdef SGX_COUNTERS
    ocall_get_sgx_counters("Destroy enclave");
#endif
    sgx_destroy_enclave(global_eid);
    delete_relation(&tableR);
    delete_relation(&tableS);
    if (params.seal)
    {
        free(sealed_R);
        free(sealed_S);
        free(sealed_result);
    }
#ifdef ITT_NOTIFS
    ocall_itt_event_end(0);
#endif
    return 0;
}
