#include "no_partitioning_join.h"
#include "npj_types.h"
#include "npj_params.h"
#include <cstdlib>
#include "pthread.h"
#include "barrier.h"
#include "util.h"
#include <cstring>

#ifdef NATIVE_COMPILATION
#include <malloc.h>
#include <util.h>
#include <cstring>
#include "lock.h"
#include "Logger.h"
#include "native_ocalls.h"
#include "pcm_commons.h"
#else
#include "Enclave_t.h" /* print_string */
#include "Enclave.h"
#endif

#ifndef NEXT_POW_2
/**
 *  compute the next number, greater than or equal to 32-bit unsigned v.
 *  taken from "bit twiddling hacks":
 *  http://graphics.stanford.edu/~seander/bithacks.html
 */
#define NEXT_POW_2(V)                           \
    do {                                        \
        V--;                                    \
        V |= V >> 1;                            \
        V |= V >> 2;                            \
        V |= V >> 4;                            \
        V |= V >> 8;                            \
        V |= V >> 16;                           \
        V++;                                    \
    } while(0)
#endif

#ifndef HASH
#define HASH(X, MASK, SKIP) (((X) & MASK) >> SKIP)
#endif

struct arg_t {
    int32_t             tid;
    hashtable_t *       ht;
    struct table_t      relR;
    struct table_t      relS;
    pthread_barrier_t * barrier;
    int64_t             num_results;
#ifndef NO_TIMING
    /* stats about the thread */
    uint64_t timer1, timer2, timer3;
    uint64_t start, end;
//    struct timeval start, end;
#endif
} ;

/**
 * Initializes a new bucket_buffer_t for later use in allocating
 * buckets when overflow occurs.
 *
 * @param ppbuf [in,out] bucket buffer to be initialized
 */
void init_bucket_buffer(bucket_buffer_t ** ppbuf)
{
    bucket_buffer_t * overflowbuf;
    overflowbuf = (bucket_buffer_t*) malloc(sizeof(bucket_buffer_t));
    if (!overflowbuf) {
        logger(ERROR, "Memory alloc for overflobuf failed!");
        ocall_exit(EXIT_FAILURE);
    }
    overflowbuf->count = 0;
    overflowbuf->next  = nullptr;

    *ppbuf = overflowbuf;
}

/**
 * Returns a new bucket_t from the given bucket_buffer_t.
 * If the bucket_buffer_t does not have enough space, then allocates
 * a new bucket_buffer_t and adds to the list.
 *
 * @param result [out] the new bucket
 * @param buf [in,out] the pointer to the bucket_buffer_t pointer
 */
inline void get_new_bucket(bucket_t ** result, bucket_buffer_t ** buf)
{
    if((*buf)->count < OVERFLOW_BUF_SIZE) {
        *result = (*buf)->buf + (*buf)->count;
        (*buf)->count ++;
    }
    else {
        /* need to allocate new buffer */
        auto * new_buf = (bucket_buffer_t*)
                malloc(sizeof(bucket_buffer_t));
        malloc_check(new_buf);
        new_buf->count = 1;
        new_buf->next  = *buf;
        *buf    = new_buf;
        *result = new_buf->buf;
    }
}

/** De-allocates all the bucket_buffer_t */
void free_bucket_buffer(bucket_buffer_t * buf)
{
    do {
        bucket_buffer_t * tmp = buf->next;
        free(buf);
        buf = tmp;
    } while(buf);
}

void allocate_hashtable(hashtable_t ** ppht, uint32_t nbuckets)
{
    hashtable_t * ht;

    ht              = (hashtable_t*)malloc(sizeof(hashtable_t));
    ht->num_buckets = nbuckets;
    NEXT_POW_2((ht->num_buckets));

    /* allocate hashtable buckets cache line aligned */
    ht->buckets = (bucket_t*) memalign(CACHE_LINE_SIZE, ht->num_buckets * sizeof(bucket_t));
    malloc_check(ht)
    malloc_check(ht->buckets);

    memset(ht->buckets, 0, ht->num_buckets * sizeof(bucket_t));
    ht->skip_bits = 0; /* the default for modulo hash */
    ht->hash_mask = (ht->num_buckets - 1) << ht->skip_bits;
    *ppht = ht;
}

/**
 * Releases memory allocated for the hashtable.
 *
 * @param ht pointer to hashtable
 */
void
destroy_hashtable(hashtable_t * ht)
{
    free(ht->buckets);
    free(ht);
}

/**
 * Single-thread hashtable build method, ht is pre-allocated.
 *
 * @param ht hastable to be built
 * @param rel the build relation
 */
void
build_hashtable_st(hashtable_t *ht, struct table_t *rel)
{
    uint64_t i;
    const uint32_t hashmask = ht->hash_mask;
    const uint32_t skipbits = ht->skip_bits;

    for(i=0; i < rel->num_tuples; i++){
        struct row_t * dest;
        bucket_t * curr, * nxt;
        int64_t idx = HASH(rel->tuples[i].key, hashmask, skipbits);

        /* copy the tuple to appropriate hash bucket */
        /* if full, follow nxt pointer to find correct place */
        curr = ht->buckets + idx;
        nxt  = curr->next;

        if(curr->count == BUCKET_SIZE) {
            if(!nxt || nxt->count == BUCKET_SIZE) {
                bucket_t * b;
                b = (bucket_t*) calloc(1, sizeof(bucket_t));
                curr->next = b;
                b->next = nxt;
                b->count = 1;
                dest = b->tuples;
            }
            else {
                dest = nxt->tuples + nxt->count;
                nxt->count ++;
            }
        }
        else {
            dest = curr->tuples + curr->count;
            curr->count ++;
        }
        *dest = rel->tuples[i];
    }
}

/**
 * Probes the hashtable for the given outer relation, returns num results.
 * This probing method is used for both single and multi-threaded version.
 *
 * @param ht hashtable to be probed
 * @param rel the probing outer relation
 *
 * @return number of matching tuples
 */
int64_t
probe_hashtable(hashtable_t *ht, struct table_t *rel)
{
    uint64_t i, j;
    int64_t matches;

    const uint32_t hashmask = ht->hash_mask;
    const uint32_t skipbits = ht->skip_bits;
#ifdef PREFETCH_NPJ
    size_t prefetch_index = PREFETCH_DISTANCE;
#endif

    matches = 0;

    for (i = 0; i < rel->num_tuples; i++)
    {
#ifdef PREFETCH_NPJ
        if (prefetch_index < rel->num_tuples) {
			intkey_t idx_prefetch = HASH(rel->tuples[prefetch_index++].key,
                                         hashmask, skipbits);
			__builtin_prefetch(ht->buckets + idx_prefetch, 0, 1);
        }
#endif

        type_key idx = HASH(rel->tuples[i].key, hashmask, skipbits);
        bucket_t * b = ht->buckets+idx;

        do {
            for(j = 0; j < b->count; j++) {
                if(rel->tuples[i].key == b->tuples[j].key){
                    matches ++;
                    /* we don't materialize the results. */
                }
            }

            b = b->next;/* follow overflow pointer */
        } while(b);
    }

    return matches;
}

/** print out the execution time statistics of the join */
static void
print_timing(uint64_t start, uint64_t end, uint64_t total, uint64_t build,
             uint64_t numtuples, int64_t result)
{
    double cyclestuple = (double) total / (double) numtuples;
    uint64_t time_usec = end - start;
    double throughput =  (double)numtuples / (double)time_usec;
    logger(INFO, "Total input tuples : %lu", numtuples);
    logger(INFO, "Result tuples : %lu", result);
    logger(INFO, "Phase Total (cycles) : %lu", total);
    logger(INFO, "Phase Build (cycles) : %lu", build);
    logger(INFO, "Phase Probe (cycles) : %lu", total-build);
    logger(INFO, "Cycles-per-tuple : %.4lf", cyclestuple);
    logger(INFO, "Total Runtime (us) : %lu ", time_usec);
    logger(INFO, "Throughput (M rec/sec) : %.2lf", throughput);
#ifdef SGX_COUNTERS
    uint64_t ewb;
    ocall_get_total_ewb(&ewb);
    logger(DBG, "EWB : %lu", ewb);
#endif

}

/**
 * Multi-thread hashtable build method, ht is pre-allocated.
 * Writes to buckets are synchronized via latches.
 *
 * @param ht hastable to be built
 * @param rel the build relation
 * @param overflowbuf pre-allocated chunk of buckets for overflow use.
 */
void build_hashtable_mt(hashtable_t *ht, struct table_t *rel,
                        bucket_buffer_t ** overflowbuf)
{
    uint64_t i;
    const uint32_t hashmask = ht->hash_mask;
    const uint32_t skipbits = ht->skip_bits;

#ifdef PREFETCH_NPJ
    size_t prefetch_index = PREFETCH_DISTANCE;
#endif

    for(i=0; i < rel->num_tuples; i++){
        struct row_t * dest;
        bucket_t * curr, * nxt;

#ifdef PREFETCH_NPJ
        if (prefetch_index < rel->num_tuples) {
            intkey_t idx_prefetch = HASH(rel->tuples[prefetch_index++].key,
                                         hashmask, skipbits);
			__builtin_prefetch(ht->buckets + idx_prefetch, 1, 1);
        }
#endif

        int32_t idx = HASH(rel->tuples[i].key, hashmask, skipbits);
        /* copy the tuple to appropriate hash bucket */
        /* if full, follow nxt pointer to find correct place */
        curr = ht->buckets+idx;
#ifdef NATIVE_COMPILATION
        lock(&curr->latch);
#else
        sgx_spin_lock(&curr->latch);
#endif
        nxt = curr->next;

        if(curr->count == BUCKET_SIZE) {
            if(!nxt || nxt->count == BUCKET_SIZE) {
                bucket_t * b;
                /* b = (bucket_t*) calloc(1, sizeof(bucket_t)); */
                /* instead of calloc() everytime, we pre-allocate */
                get_new_bucket(&b, overflowbuf);
                curr->next = b;
                b->next    = nxt;
                b->count   = 1;
                dest       = b->tuples;
            }
            else {
                dest = nxt->tuples + nxt->count;
                nxt->count ++;
            }
        }
        else {
            dest = curr->tuples + curr->count;
            curr->count ++;
        }

        *dest = rel->tuples[i];
#ifdef NATIVE_COMPILATION
        unlock(&curr->latch);
#else
        sgx_spin_unlock(&curr->latch);
#endif
    }

}

/**
 * Just a wrapper to call the build and probe for each thread.
 *
 * @param param the parameters of the thread, i.e. tid, ht, reln, ...
 *
 * @return
 */
void *
npo_thread(void * param)
{
    int rv = 0;
    auto * args = (arg_t*) param;

    /* allocate overflow buffer for each thread */
    bucket_buffer_t * overflowbuf;
    init_bucket_buffer(&overflowbuf);

#ifdef PCM_COUNT
    if (args->tid == 0) {
        ocall_set_system_counter_state("Start build phase");
    }
#endif

    /* wait at a barrier until each thread starts and start timer */
    barrier_arrive(args->barrier, rv);

#ifndef NO_TIMING
    /* the first thread checkpoints the start time */
    if(args->tid == 0){
        args->timer3 = 0; /* no partitionig phase */

        ocall_get_system_micros(&args->start);
        ocall_startTimer(&args->timer1);
        ocall_startTimer(&args->timer2);
    }
#endif

    /* insert tuples from the assigned part of relR to the ht */
    build_hashtable_mt(args->ht, &args->relR, &overflowbuf);

    /* wait at a barrier until each thread completes build phase */
    barrier_arrive(args->barrier, rv);

#ifdef PCM_COUNT
    if (args->tid == 0) {
        ocall_get_system_counter_state("Build", 0);
        ocall_set_system_counter_state("Start probe");
    }
    barrier_arrive(args->barrier, rv);
#endif

#ifndef NO_TIMING
    /* build phase finished, thread-0 checkpoints the time */
    if(args->tid == 0){
        ocall_stopTimer(&args->timer2);
    }
#endif

    /* probe for matching tuples from the assigned part of relS */
    args->num_results = probe_hashtable(args->ht, &args->relS);

#ifndef NO_TIMING
    /* for a reliable timing we have to wait until all finishes */
    barrier_arrive(args->barrier, rv);

    /* probe phase finished, thread-0 checkpoints the time */
    if(args->tid == 0){
        ocall_stopTimer(&args->timer1);
        ocall_get_system_micros(&args->end);
    }
#endif

#ifdef PCM_COUNT
    if (args->tid == 0) {
        ocall_get_system_counter_state("Probe", 0);
    }
    barrier_arrive(args->barrier, rv);
#endif

    /* clean-up the overflow buffers */
    free_bucket_buffer(overflowbuf);

    return 0;
}

result_t* PHT (struct table_t *relR, struct table_t *relS, joinconfig_t * config)
{
    hashtable_t * ht;
    int64_t result = 0;
    uint32_t numR, numS, numRthr, numSthr; /* total and per thread num */
    int i, rv;
    int nthreads = config->NTHREADS;
    arg_t args[nthreads];
    pthread_t tid[nthreads];
    pthread_barrier_t barrier;

    auto nbuckets = (uint32_t)(relR->num_tuples / BUCKET_SIZE);
    allocate_hashtable(&ht, nbuckets);

    numR = (uint32_t) relR->num_tuples;
    numS = (uint32_t) relS->num_tuples;
    numRthr = numR / nthreads;
    numSthr = numS / nthreads;

    rv = pthread_barrier_init(&barrier, NULL, nthreads);
    if(rv != 0){
        logger(DBG, "Couldn't create the barrier");
        ocall_exit(EXIT_FAILURE);
    }

    for(i = 0; i < nthreads; i++){
        args[i].tid = i;
        args[i].ht = ht;
        args[i].barrier = &barrier;

        /* assing part of the relR for next thread */
        args[i].relR.num_tuples = (i == (nthreads - 1)) ? numR : numRthr;
        args[i].relR.tuples = relR->tuples + numRthr * i;
        numR -= numRthr;

        /* assing part of the relS for next thread */
        args[i].relS.num_tuples = (i == (nthreads - 1)) ? numS : numSthr;
        args[i].relS.tuples = relS->tuples + numSthr * i;
        numS -= numSthr;

        rv = pthread_create(&tid[i], nullptr, npo_thread, (void*)&args[i]);
        if (rv){
            logger(ERROR, "ERROR; return code from pthread_create() is %d\n", rv);
            ocall_exit(-1);
        }

    }

    for(i = 0; i < nthreads; i++){
        pthread_join(tid[i], nullptr);
        /* sum up results */
        result += args[i].num_results;
    }


#ifndef NO_TIMING
    if (config->PRINT) {
        /* now print the timing results: */
        print_timing(args[0].start, args[0].end, args[0].timer1, args[0].timer2,
                     relR->num_tuples + relS->num_tuples, result);
    }
#endif

    destroy_hashtable(ht);

    result_t * joinresult;
    joinresult = (result_t *) malloc(sizeof(result_t));
    joinresult->totalresults = result;
    joinresult->nthreads = nthreads;

    return joinresult;
}

result_t* BHJ (relation_t *relR, relation_t *relS, joinconfig_t *config)
{
    uint64_t timerStart, timerEnd;
    ocall_get_system_micros(&timerStart);
    // start with calculating the number of scans
    uint32_t scans = 4;

    uint64_t numPerScan = relR->num_tuples / scans;
    config->PRINT = 0;

    auto *jr = (result_t*) malloc(sizeof(result_t));
    jr->resultlist = (threadresult_t *) malloc(sizeof(threadresult_t) * config->NTHREADS);
    malloc_check(jr);
    malloc_check(jr->resultlist);

    jr->nthreads = config->NTHREADS;
    jr->totalresults = 0;

    for (uint32_t i = 0; i < scans; i++) {
        uint64_t numR = (i == (scans - 1)) ?
                        (relR->num_tuples - i * numPerScan) : numPerScan;
        table_t RR {};
        RR.num_tuples = numR;
        RR.tuples = relR->tuples + i * numPerScan;
        result_t *tmp = PHT(&RR, relS, config);
        jr->totalresults += tmp->totalresults;
    }
    ocall_get_system_micros(&timerEnd);
    print_timing(timerStart, timerEnd, 0, 0,
                 relR->num_tuples + relS->num_tuples, jr->totalresults);
    double throughput =  (double)(relR->num_tuples + relS->num_tuples) / (double)(timerEnd-timerStart);
    jr->throughput = throughput;
    return jr;
}