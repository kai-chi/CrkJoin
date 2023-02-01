#include <stdint.h>
#include "btree.h"
#include <pthread.h>
#include "data-types.h"
#ifdef NATIVE_COMPILATION
#include "Logger.h"
#include "native_ocalls.h"
#else
#include "Enclave_t.h"
#include "Enclave.h"
#endif

typedef struct arg_nl_t {
    tuple_t * relR;
    tuple_t * relS;

    uint64_t numR;
    uint64_t numS;

    uint64_t result;
    int32_t my_tid;
} arg_nl_t;

static void
print_timing(uint64_t total_cycles, uint64_t numtuples, uint64_t join_matches,
                  uint64_t start, uint64_t end)
{
    double cyclestuple = (double) total_cycles / (double) numtuples;
    uint64_t time_usec = end - start;
    double throughput = (double)numtuples / (double) time_usec;

    logger(ENCLAVE, "Total input tuples     : %lu", numtuples);
    logger(ENCLAVE, "Result tuples          : %lu", join_matches);
    logger(ENCLAVE, "Phase Join (cycles)    : %lu", total_cycles);
    logger(ENCLAVE, "Cycles-per-tuple       : %.4lf", cyclestuple);
    logger(ENCLAVE, "Total Runtime (us)     : %lu ", time_usec);
    logger(ENCLAVE, "Throughput (M rec/sec) : %.2lf", throughput);
}

void * nlj_thread(void * param) {
    arg_nl_t *args = (arg_nl_t*) param;
    uint64_t results = 0;

    for (uint64_t i = 0; i < args->numR; i++)
    {
        for (uint64_t j = 0; j < args->numS; j++)
        {
            if (args->relR[i].key == args->relS[j].key)
            {
                results++;
            }
        }
    }

    args->result = results;
    return nullptr;
}

result_t* NL (struct table_t* relR, struct table_t* relS, joinconfig_t * config) {

    int nthreads = config->NTHREADS;
    int64_t result = 0;
    pthread_t tid[nthreads];
    arg_nl_t args[nthreads];
    uint64_t numperthr[2];
#ifndef NO_TIMING
    uint64_t timer1, start, end;
    ocall_startTimer(&timer1);
    ocall_get_system_micros(&start);
#endif
#ifdef PCM_COUNT
    ocall_set_system_counter_state("Start join phase");
#endif

    numperthr[0] = relR->num_tuples / nthreads;
    numperthr[1] = relS->num_tuples / nthreads;

    for (int i = 0; i < nthreads; i++) {
        args[i].my_tid = i;
        args[i].relR = relR->tuples + i * numperthr[0];
        args[i].relS = relS->tuples;
        args[i].numR = (i == (nthreads-1)) ?
                       (relR->num_tuples - i * numperthr[0]) : numperthr[0];
        args[i].numS = relS->num_tuples;
        args[i].result = 0;

        int rv = pthread_create(&tid[i], nullptr, nlj_thread, (void*)&args[i]);
        if (rv){
            logger(ERROR, "return code from pthread_create() is %d\n", rv);
            ocall_exit(-1);
        }
    }

    for (int i = 0; i < nthreads; i++) {
        pthread_join(tid[i], NULL);
        result += args[i].result;
    }

#ifdef PCM_COUNT
    ocall_get_system_counter_state("Join", 0);
#endif

#ifndef NO_TIMING
    ocall_stopTimer(&timer1);
    ocall_get_system_micros(&end);
    print_timing(timer1, relR->num_tuples + relS->num_tuples, result, start, end);
#endif

    result_t * joinresult;
    joinresult = (result_t *) malloc(sizeof(result_t));
    joinresult->totalresults = result;
    joinresult->nthreads = nthreads;
    return joinresult;
}
