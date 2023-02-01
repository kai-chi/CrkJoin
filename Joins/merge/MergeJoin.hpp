#ifndef MERGEJOIN_HPP
#define MERGEJOIN_HPP

static void
print_timing(uint64_t total_cycles, uint64_t numtuples, uint64_t join_matches,
             uint64_t start, uint64_t end)
{
    double cyclestuple = (double) total_cycles / (double) numtuples;
    uint64_t time_usec = end - start;
    double throughput = (double)numtuples / (double) time_usec;

    logger(ENCLAVE, "Total input tuples     : %lu", numtuples);
    logger(ENCLAVE, "Result tuples          : %lu", join_matches);
    logger(ENCLAVE, "Cycles-per-tuple       : %.4lf", cyclestuple);
    logger(ENCLAVE, "Total Runtime (us)     : %lu ", time_usec);
    logger(ENCLAVE, "Throughput (M rec/sec) : %.2lf", throughput);
}

result_t * MJ (relation_t *relR, relation_t *relS, joinconfig_t *config)
{
    relation_t *R, *S;
    R = (relation_t *) malloc(sizeof(relation_t));
    R->num_tuples = relR->num_tuples;
    R->tuples = (tuple_t*) malloc(sizeof(tuple_t) * R->num_tuples);
    memcpy(R->tuples, relR->tuples, R->num_tuples * sizeof(tuple_t));

    S = (relation_t *) malloc(sizeof(relation_t));
    S->num_tuples = relS->num_tuples;
    S->tuples = (tuple_t*) malloc(sizeof(tuple_t) * S->num_tuples);
    memcpy(S->tuples, relS->tuples, S->num_tuples * sizeof(tuple_t));

    uint64_t start, end, timer;
    ocall_get_system_micros(&start);
    ocall_startTimer(&timer);
    tuple_t *outer = relR->tuples;
    tuple_t *inner = relS->tuples;
    tuple_t *mark;
    uint64_t matches = 0;

    tuple_t *olast = outer + relR->num_tuples;
    tuple_t *ilast = inner + relS->num_tuples;

    while(outer < olast && inner < ilast) {
        while(outer->key != inner->key) {
            if (outer->key < inner->key) {
                outer++;
            } else {
                inner++;
            }
        }
        mark = inner;
        while(true) {
            while(outer->key == inner->key) {
                matches++;
                inner++;
            }
            outer++;
            if (outer == mark) {
                inner = mark;
            } else {
                break;
            }
        }
    }

    ocall_get_system_micros(&end);
    ocall_stopTimer(&timer);
    print_timing(timer, relR->num_tuples + relS->num_tuples, matches, start, end);

    free(R->tuples);
    free(R);
    free(S->tuples);
    free(S);

    result_t * joinresult;
    joinresult = (result_t *) malloc(sizeof(result_t));
    joinresult->totalresults = matches;
    joinresult->nthreads = config->NTHREADS;
    return joinresult;
}
#endif //MERGEJOIN_HPP
