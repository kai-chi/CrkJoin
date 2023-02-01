#include <util.h>
#include "Enclave_t.h"
#include "TpcHTypes.hpp"
#include "data-types.h"
#include "Enclave.h"
#include "Q3Predicates.hpp"
#include "Q10Predicates.hpp"
#include "Q12Predicates.hpp"
#include "Q19Predicates.hpp"
#include <pthread.h>

void print_query_results(uint64_t totalTime, uint64_t filterTime, uint64_t joinTime, uint64_t numTuples);

uint64_t q19FilterJoinResults(result_t *jr, LineItemTable *l, PartTable *p);

struct JrToTableArg
{
    uint32_t tid;
    output_list_t *threadresult;
    uint64_t nresults;
    tuple_t *out;
    type_key *keyToExtract;

    //for ST
    int nthreads;
    struct threadresult_t * resultlist;
};

typedef void * (*ToTableThread) (void * param);

void * toTableThread_RpToKeySpST(void * param)
{
    auto arg = static_cast<JrToTableArg*>(param);
    int nthreads = arg->nthreads;
    uint64_t i = 0;
    for (int j = 0; j < nthreads; j++) {
        output_list_t *tlist = arg->resultlist[j].results;
        uint64_t tresults = arg->resultlist[j].nresults;
        for (uint64_t k = 0; k < tresults; k++) {
            if (tlist == nullptr) {
                logger(ERROR, "Error retrieving join tuple");
            }
            arg->out[i].key = arg->keyToExtract[tlist->Rpayload];
            arg->out[i].payload = tlist->Spayload;
            i++;
            tlist = tlist->next;
        }
    }
    return nullptr;
}

void * toTableThread_RpToKeySp(void * param)
{
    auto *arg = static_cast<JrToTableArg*>(param);
    output_list_t *tmp = arg->threadresult;
    for (uint64_t i = 0; i < arg->nresults; i++) {
        if (tmp == nullptr) {
            logger(ERROR, "Error retrieving join tuple");
        }
        arg->out[i].key = arg->keyToExtract[tmp->Rpayload];
        arg->out[i].payload = tmp->Spayload;
        tmp = tmp->next;
    }
    return nullptr;
}

void * toTableThread_SpToTupleST(void * param)
{
    auto arg = static_cast<JrToTableArg*>(param);
    int nthreads = arg->nthreads;
    uint64_t i = 0;
    for (int j = 0; j < nthreads; j++) {
        output_list_t *tlist = arg->resultlist[j].results;
        uint64_t tresults = arg->resultlist[j].nresults;
        for (uint64_t k = 0; k < tresults; k++) {
            if (tlist == nullptr) {
                logger(ERROR, "Error retrieving join tuple");
            }
            arg->out[i].key = arg->keyToExtract[tlist->Spayload];
            i++;
            tlist = tlist->next;
        }
    }
    return nullptr;
}

void * toTableThread_SpToTuple(void * param)
{
    auto *arg = static_cast<JrToTableArg*>(param);
    output_list_t *tmp = arg->threadresult;
    for (uint64_t i = 0; i < arg->nresults; i++) {
        if (tmp == nullptr) {
            logger(ERROR, "Error retrieving join tuple");
        }
        arg->out[i].key = arg->keyToExtract[tmp->Spayload];
        tmp = tmp->next;
    }
    return nullptr;
}

void * toTableThread_SpSpST(void * param)
{
    auto arg = static_cast<JrToTableArg*>(param);
    int nthreads = arg->nthreads;
    uint64_t i = 0;
    for (int j = 0; j < nthreads; j++) {
        output_list_t *tlist = arg->resultlist[j].results;
        uint64_t tresults = arg->resultlist[j].nresults;
        for (uint64_t k = 0; k < tresults; k++) {
            if (tlist == nullptr) {
                logger(ERROR, "Error retrieving join tuple");
            }
            arg->out[i].key = tlist->Spayload;
            arg->out[i].payload = tlist->Spayload;
            i++;
            tlist = tlist->next;
        }
    }
    return nullptr;
}

void * toTableThread_SpSp(void * param)
{
    auto *arg = static_cast<JrToTableArg*>(param);
    output_list_t *tmp = arg->threadresult;
    uint64_t i = 0;
    while(tmp != nullptr) {
        arg->out[i].key = tmp->Spayload;
        arg->out[i].payload = tmp->Spayload;
        tmp = tmp->next;
        i++;
    }

    SGX_ASSERT(i == arg->nresults, "iterated over wrong number of elements");
    return nullptr;
}

void * toTableThread_RkSp(void * param)
{
    auto *arg = static_cast<JrToTableArg*>(param);
    output_list_t *tmp = arg->threadresult;
    for (uint64_t i = 0; i < arg->nresults; i++) {
        if (tmp == nullptr) {
            logger(ERROR, "Error retrieving join tuple");
        }
        arg->out[i].key = tmp->key;
        arg->out[i].payload = tmp->Spayload;
        tmp = tmp->next;
    }
    return nullptr;
}

void joinResultToTableST(table_t *table, result_t *jr, ToTableThread t_thread, type_key *keyToExtract)
{
    table->tuples = (tuple_t*) malloc(sizeof(tuple_t) * jr->totalresults);
    malloc_check(table->tuples);
    table->num_tuples = jr->totalresults;
    auto args = new JrToTableArg;
    args->tid = 0;
    args->nthreads = jr->nthreads;
    args->resultlist = jr->resultlist;
    args->out = table->tuples;
    args->keyToExtract = keyToExtract;
    t_thread((void*)args);
}

void joinResultToTable(table_t *table, result_t *jr, ToTableThread t_thread, type_key *keytoExtract)
{
    table->tuples = (tuple_t*) malloc(sizeof(tuple_t) * jr->totalresults);
    malloc_check(table->tuples);
    table->num_tuples = jr->totalresults;
    int nthreads = jr->nthreads;
    uint64_t offset[nthreads];
    offset[0] = 0;
    for (int i = 1; i < nthreads; i++) {
        offset[i] = offset[i-1] + jr->resultlist[i-1].nresults;
    }

    auto *args = new JrToTableArg[nthreads];
    pthread_t tid[nthreads];
    int rv;
    for (int i = 0; i < jr->nthreads; i++) {
        args[i].tid = jr->resultlist[i].threadid;
        args[i].threadresult = jr->resultlist[i].results;
        args[i].nresults = jr->resultlist[i].nresults;
        args[i].out = table->tuples + offset[i];
        args[i].keyToExtract = keytoExtract;
        rv = pthread_create(&tid[i], nullptr, t_thread, (void*)&args[i]);
        if (rv) {
            logger(ERROR, "return code from pthread_create() is %d\n", rv);
        }
    }
    for (int i = 0; i < nthreads; i++) {
        pthread_join(tid[i], nullptr);
    }
}

void joinResultToTableST(table_t *table, result_t *jr, ToTableThread t_thread, tuple_t *keytoExtract, uint64_t keySize)
{
    auto *tmp = (type_key*) malloc(sizeof(type_key) * keySize);
    for (uint64_t i = 0; i < keySize; i++) {
        tmp[i] = keytoExtract[i].key;
    }
    joinResultToTableST(table, jr, t_thread, tmp);
    free(tmp);
}

void joinResultToTable(table_t *table, result_t *jr, ToTableThread t_thread, tuple_t *keytoExtract, uint64_t keySize)
{
    auto *tmp = (type_key*) malloc(sizeof(type_key) * keySize);
    for (uint64_t i = 0; i < keySize; i++) {
        tmp[i] = keytoExtract[i].key;
    }
    joinResultToTable(table, jr, t_thread, tmp);
    free(tmp);
}

void joinResultToTableST(table_t *table, result_t *jr, ToTableThread t_thread)
{
    return joinResultToTableST(table, jr, t_thread, nullptr);
}

void joinResultToTable(table_t *table, result_t *jr, ToTableThread t_thread)
{
    return joinResultToTable(table, jr, t_thread, nullptr);
}

sgx_status_t ecall_tpch_q19(result_t *result,
                            LineItemTable *l,
                            PartTable *p,
                            char *algorithm,
                            joinconfig_t *config)
{
    logger(DBG, "%s", __FUNCTION__);
    logger(DBG, "LineItemTable size: %u, PartTable size: %u", l->numTuples, p->numTuples);
    uint64_t timerStart, timerEnd, timerSelection=0, timerJoin=0, tmp1, tmp2;
    ocall_get_system_micros(&timerStart);
    table_t R{}, S{};
    uint64_t selectionMatches = 0;

    //selection Part
    R.tuples = (tuple_t*) malloc(sizeof(tuple_t) * p->numTuples);//p->p_partkey;
    malloc_check(R.tuples);
    for (uint64_t i = 0; i < p->numTuples; i++) {
        if (q19PartPredicate(p, i)) {
            R.tuples[selectionMatches++] = p->p_partkey[i];
        }
    }
    R.num_tuples = selectionMatches;
    if (selectionMatches < p->numTuples) {
        R.tuples = (tuple_t *) realloc(R.tuples, sizeof(tuple_t) * selectionMatches);
        malloc_check(R.tuples);
    }

    //selection LineItem
    selectionMatches = 0;
    S.tuples = (tuple_t*) malloc(sizeof(tuple_t) * l->numTuples);
    malloc_check(S.tuples);
    for (uint64_t i = 0; i < l->numTuples; i++) {
        if (q19LineItemPredicate(l, i)) {
            S.tuples[selectionMatches].key = l->l_partkey[i];
            S.tuples[selectionMatches++].payload = l->l_orderkey[i].payload;
        }
    }
    S.num_tuples = selectionMatches;
    if (selectionMatches < l->numTuples) {
        S.tuples = (tuple_t*) realloc(S.tuples, sizeof(tuple_t) * selectionMatches);
        malloc_check(S.tuples);
    }
    ocall_get_system_micros(&tmp1);
    timerSelection += (tmp1 - timerStart);
    //join
    logger(DBG, "Join Part=%u with LineItem=%u", R.num_tuples, S.num_tuples);
    config->MATERIALIZE = true;
    config->RADIXBITS = 8;
    ecall_join(result, &R, &S, algorithm, config);
    ocall_get_system_micros(&tmp2);
    timerJoin = tmp2 - tmp1;
    logger(INFO, "Join 1 timer : %lu", (tmp2 - tmp1));

    //filter the join results
    uint64_t matches = q19FilterJoinResults(result, l, p);
    ocall_get_system_micros(&tmp1);
    timerSelection += (tmp1 - tmp2);
    logger(INFO, "Total matches = %u", matches);
    timerEnd = tmp1;
    free(R.tuples);
    free(S.tuples);
    uint64_t numTuples = (l->numTuples + p->numTuples);
    uint64_t totalTime = timerEnd - timerStart;
    print_query_results(totalTime, timerSelection, timerJoin, numTuples);
    result->throughput = static_cast<double>(numTuples) / static_cast<double>(totalTime);
    return SGX_SUCCESS;
}

struct Q19ThreadArg
{
    uint64_t matches;
    output_list_t *threadresult;
    LineItemTable *l;
    PartTable *p;
    uint64_t nresults;
};

void * q19FilterThread(void * param)
{
    auto *arg = static_cast<Q19ThreadArg*>(param);
    output_list_t *head = arg->threadresult;
    while (head != nullptr) {
        type_value rowIdPart = head->Rpayload;
        type_value rowIdLineItem = head->Spayload;
        if (q19FinalPredicate(arg->p, rowIdPart, arg->l, rowIdLineItem)) {
            arg->matches++;
        }
        head = head->next;
    }
    return nullptr;
}

uint64_t q19FilterJoinResults(result_t *jr, LineItemTable *l, PartTable *p)
{
    int nthreads = jr->nthreads;
    pthread_t tid[nthreads];
    auto *args = new Q19ThreadArg[nthreads];

    int rv = 0;
    for (int i = 0; i < nthreads; i++) {
        args[i].threadresult = jr->resultlist[i].results;
        args[i].nresults = jr->resultlist[i].nresults;
        args[i].p = p;
        args[i].l = l;
        args[i].matches = 0;
        rv = pthread_create(&tid[i], nullptr, q19FilterThread, (void*)&args[i]);
        if (rv) {
            logger(ERROR, "return code from pthread_create() is %d\n", rv);
        }
    }
    for (int i = 0; i < nthreads; i++) {
        pthread_join(tid[i], nullptr);
    }
    uint64_t totalMatches = 0;
    for (int i = 0; i < nthreads; i++) {
        totalMatches += args[i].matches;
    }

    delete []args;
    return totalMatches;
}

sgx_status_t ecall_tpch_q10(result_t *result,
                            CustomerTable *c,
                            OrdersTable *o,
                            LineItemTable *l,
                            NationTable *n,
                            char *algorithm,
                            joinconfig_t *config)
{
    uint64_t timerStart=0, timerEnd=0, timerSelection=0, timerJoin=0, tmp1=0, tmp2=0;
    logger(DBG, "%s", __FUNCTION__);
    logger(DBG, "CustomerTable: %u, OrdersTable size: %u, LineItemTable size: %u, NationTable: %u",
           c->numTuples, o->numTuples, l->numTuples, n->numTuples);

    ocall_get_system_micros(&timerStart);
    table_t R{}, S{};
    uint64_t selectionMatches = 0;
    //filter customer
    R.tuples = c->c_custkey;
    R.num_tuples = c->numTuples;
    //filter orders
    S.tuples = (tuple_t*) malloc(sizeof(tuple_t) * o->numTuples);
    malloc_check(S.tuples);
    for (uint64_t i = 0; i < o->numTuples; i++) {
        if (q10OrdersPredicate(o, i)) {
            S.tuples[selectionMatches].key = o->o_custkey[i];
            S.tuples[selectionMatches++].payload = o->o_orderkey[i].payload;
        }
    }
    S.num_tuples = selectionMatches;
    if (selectionMatches < o->numTuples) {
        S.tuples = (tuple_t*) realloc(S.tuples, sizeof(tuple_t) * selectionMatches);
        malloc_check(S.tuples);
    }
    ocall_get_system_micros(&tmp1);
    timerSelection = tmp1 - timerStart;

    //join orders and customer
    logger(DBG, "Join Customer=%u with Orders=%u",
           R.num_tuples, S.num_tuples);
    config->MATERIALIZE = true;
    result_t joinResult;
    ecall_join(&joinResult, &R, &S, algorithm, config);
    ocall_get_system_micros(&tmp2);
    timerJoin = (tmp2 - tmp1);
    logger(INFO, "Join 1 timer : %lu", timerJoin);
    free(S.tuples);
    //transform joinResult to relation_t
    table_t U{};
//    joinResultToTable(&U, &joinResult, toTableThread_RpToKeySp, c->c_nationkey);
    joinResultToTableST(&U, &joinResult, toTableThread_RpToKeySpST, c->c_nationkey);
    logger(INFO, "U tuples=%u", U.num_tuples);
    delete[] joinResult.resultlist;
    ocall_get_system_micros(&tmp1);
    timerSelection += (tmp1 - tmp2);

    //join nation and previous
    R.tuples = n->n_nationkey;
    R.num_tuples = n->numTuples;
    config->MATERIALIZE = true;
    config->RADIXBITS = 3;
    logger(DBG, "Join Nation=%u with U=%u", n->numTuples, U.num_tuples);
    ecall_join(&joinResult, &R, &U, algorithm, config);
    ocall_get_system_micros(&tmp2);
    timerJoin += (tmp2 - tmp1);
    logger(INFO, "Join 2 timer : %lu", (tmp2 - tmp1));
    free(U.tuples);
//    joinResultToTable(&U, &joinResult, toTableThread_SpToTuple, o->o_orderkey, o->numTuples);
    joinResultToTableST(&U, &joinResult, toTableThread_SpToTupleST, o->o_orderkey, o->numTuples);
    delete[] joinResult.resultlist;

    //filter lineitem
    table_t V{};
    V.tuples = (tuple_t*) malloc(sizeof(tuple_t) * l->numTuples);
    malloc_check(V.tuples);
    selectionMatches = 0;
    for (uint64_t i = 0; i < l->numTuples; i++) {
        if (q10LineItemPredicate(l, i)) {
            V.tuples[selectionMatches++] = l->l_orderkey[i];
        }
    }
    V.num_tuples = selectionMatches;
    if (selectionMatches < l->numTuples) {
        V.tuples = (tuple_t*) realloc(V.tuples, sizeof(tuple_t) * selectionMatches);
        malloc_check(V.tuples);
    }
    ocall_get_system_micros(&tmp1);
    timerSelection += (tmp1 - tmp2);

    //join lineitem and previous
    logger(DBG, "Join U=%u with LineItem=%u", U.num_tuples, V.num_tuples);
    config->MATERIALIZE = false;
    config->RADIXBITS = 8;
    ecall_join(result, &U, &V, algorithm, config);
    ocall_get_system_micros(&timerEnd);
    timerJoin += (timerEnd - tmp1);
    logger(INFO, "Join 3 timer : %lu", (timerEnd - tmp1));

    free(U.tuples);
    free(V.tuples);

    uint64_t numTuples = (l->numTuples + o->numTuples + c->numTuples + n->numTuples);
    uint64_t totalTime = timerEnd - timerStart;
    print_query_results(totalTime, timerSelection, timerJoin, numTuples);
    result->throughput = static_cast<double>(numTuples) / static_cast<double>(totalTime);

    return SGX_SUCCESS;
}

sgx_status_t ecall_tpch_q12(result_t *result,
                            LineItemTable *l,
                            OrdersTable *o,
                            char *algorithm,
                            joinconfig_t *config)
{

    logger(DBG, "%s", __FUNCTION__);
    logger(DBG, "LineItemTable size: %u, OrdersTable size: %u", l->numTuples, o->numTuples);
    uint64_t timerStart, timerEnd, timerSelection, timerJoin, tmp;
    ocall_get_system_micros(&timerStart);
    table_t R{}, S{};
    R.tuples = o->o_orderkey;
    R.num_tuples = o->numTuples;

    // filter linetitem
    S.tuples = (tuple_t *) malloc(sizeof(tuple_t) * l->numTuples);
    malloc_check(S.tuples);
    uint64_t selectionMatches = 0;
    for (uint64_t i = 0; i < l->numTuples; i++) {
        if (q12Predicate(l, i)) {
            S.tuples[selectionMatches++] = l->l_orderkey[i];
        }
    }
    S.num_tuples = selectionMatches;
    if (selectionMatches < l->numTuples) {
        S.tuples = (tuple_t *) realloc(S.tuples, (sizeof(tuple_t) * selectionMatches));
        malloc_check(S.tuples);
    }
    ocall_get_system_micros(&tmp);
    timerSelection = tmp - timerStart;
    // join l and o
    logger(DBG, "Join R=%u with S=%u tuples", R.num_tuples, S.num_tuples);
    config->MATERIALIZE = false;
    ecall_join(result, &R, &S, algorithm, config);
    ocall_get_system_micros(&timerJoin);
    logger(INFO, "Join 1 timer : %lu", (timerJoin - tmp));
    timerJoin -= tmp;

    // materialize output


    // clean up
    free(S.tuples);
    ocall_get_system_micros(&timerEnd);
    uint64_t numTuples = (l->numTuples + o->numTuples);
    uint64_t totalTime = timerEnd - timerStart;
    print_query_results(totalTime, timerSelection, timerJoin, numTuples);
    result->throughput = static_cast<double>(numTuples) / static_cast<double>(totalTime);

    return SGX_SUCCESS;
}

sgx_status_t ecall_tpch_q3(result_t *result,
                           struct CustomerTable *c,
                           struct OrdersTable *o,
                           struct LineItemTable *l,
                           char *algorithm,
                           struct joinconfig_t *config)
{
    logger(DBG, "%s", __FUNCTION__);
    logger(DBG, "LineItemTable size: %u, OrdersTable size: %u, CustomerTable size: %u",
           l->numTuples, o->numTuples, c->numTuples);

    table_t R{}, S{}, T{};
    uint64_t selectionMatches = 0;
    uint64_t i;
    uint64_t timerStart=0, timerEnd=0, timerSelection=0, timerJoin=0, tmp1=0, tmp2=0;
    ocall_get_system_micros(&timerStart);

    //selection on customers
    R.tuples = (tuple_t*) malloc(sizeof(tuple_t) * c->numTuples);
    malloc_check(R.tuples)
    for (i = 0; i < c->numTuples; i++) {
        if (q3CustomerPredicate(c, i)) {
            R.tuples[selectionMatches++] = c->c_custkey[i];
        }
    }
    R.num_tuples = selectionMatches;
    if (selectionMatches < c->numTuples) {
        R.tuples = (tuple_t*) realloc(R.tuples, (sizeof(tuple_t) * selectionMatches));
        malloc_check(R.tuples);
    }

    //selection on orders
    selectionMatches = 0;
    S.tuples = (tuple_t*) malloc(sizeof(tuple_t) * o->numTuples);
    malloc_check(S.tuples);
    for (i = 0; i < o->numTuples; i++) {
        if (q3OrdersPredicate(o, i)) {
            S.tuples[selectionMatches].key = o->o_custkey[i];
            S.tuples[selectionMatches++].payload = o->o_orderkey[i].key;
        }
    }
    S.num_tuples = selectionMatches;
    if (selectionMatches < o->numTuples) {
        S.tuples = (tuple_t*) realloc(S.tuples, (sizeof(tuple_t) * selectionMatches));
        malloc_check(S.tuples);
    }
    ocall_get_system_micros(&tmp1);
    timerSelection = tmp1 - timerStart;
    //join customers and orders
    logger(DBG, "Join customers=%u with orders=%u", R.num_tuples, S.num_tuples);
    result_t joinResult;
    config->MATERIALIZE = true;
//    config->RADIXBITS = 8;
    ecall_join(&joinResult, &R, &S, algorithm, config);
    ocall_get_system_micros(&tmp2);
    timerJoin = (tmp2 - tmp1);
    logger(INFO, "Join 1 timer : %lu", timerJoin);
    free(R.tuples);
    free(S.tuples);
    //transform joinResult to relation_t
    table_t U{};
//    joinResultToTable(&U, &joinResult, toTableThread_SpSp);
    joinResultToTableST(&U, &joinResult, toTableThread_SpSpST);
    logger(INFO, "U tuples=%u", U.num_tuples);
    //selection on lineitem
    selectionMatches = 0;
    T.tuples = (tuple_t*) malloc(sizeof(tuple_t) * l->numTuples);
    malloc_check(T.tuples);
    for (i = 0; i < l->numTuples; i++) {
        if (q3LineitemPredicate(l, i)) {
            T.tuples[selectionMatches++] = l->l_orderkey[i];
        }
    }
    T.num_tuples = selectionMatches;
    if (selectionMatches < l->numTuples) {
        T.tuples = (tuple_t *) realloc(T.tuples, (sizeof(tuple_t) * selectionMatches));
        malloc_check(T.tuples);
    }
    ocall_get_system_micros(&tmp1);
    timerSelection += (tmp1 - tmp2);
    //join with lineitems
    logger(DBG, "Join U=%u with lineitems=%u", U.num_tuples, T.num_tuples);
    config->MATERIALIZE = false;
    ecall_join(result, &U, &T, algorithm, config);
    ocall_get_system_micros(&timerEnd);
    timerJoin += (timerEnd - tmp1);
    logger(INFO, "Join 2 timer : %lu", (timerEnd - tmp1));
    //clean up
    free(T.tuples);
    free(U.tuples);

    uint64_t numTuples = (l->numTuples + o->numTuples + c->numTuples);
    uint64_t totalTime = timerEnd - timerStart;
    print_query_results(totalTime, timerSelection, timerJoin, numTuples);
    result->throughput = static_cast<double>(numTuples) / static_cast<double>(totalTime);

    return SGX_SUCCESS;
}

void print_query_results(uint64_t totalTime, uint64_t filterTime, uint64_t joinTime, uint64_t numTuples)
{
    logger(INFO, "QueryTimeTotal (us)         : %u", totalTime);
    logger(INFO, "QueryTimeSelection (us)     : %u (%.2lf%%)", filterTime,
           (100 * (double) filterTime / (double) totalTime));
    logger(INFO, "QueryTimeJoin (us)          : %u (%.2lf%%)", joinTime,
           (100 * (double) joinTime / (double) totalTime));
    logger(INFO, "QueryThroughput (M rec/s)   : %.4lf",
           static_cast<double>(numTuples) / static_cast<double>(totalTime));
}