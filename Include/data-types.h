#ifndef DATA_TYPES_H
#define DATA_TYPES_H

#include <stdint.h>
#include <stdbool.h>

#ifdef KEY_8B /* 64-bit key/payload, 16B tuples */
typedef uint64_t intkey_t;
typedef uint64_t value_t;
#else /* 32-bit key/payload, 8B tuples */
typedef uint32_t intkey_t;
typedef uint32_t value_t;
#endif

typedef uint64_t intkey64_t;
typedef uint64_t intkey64_t;

#if !defined PRId64
# undef PRId64
# define PRId64 "lld"
#endif

#if !defined PRIu64
# undef PRIu64
# define PRIu64 "llu"
#endif

#ifndef B_TO_MB
#define B_TO_MB(X) ((double)X/1024.0/1024.0)
#endif

//#ifndef __forceinline
//#define __forceinline __attribute__((always_inline))
//#endif

typedef uint32_t type_key;
typedef uint32_t type_value;

typedef struct row_t tuple_t;
typedef struct output_list_t output_list_t;
typedef struct output_t output_t;
typedef struct table_t relation_t;
typedef struct result_t result_t;
typedef struct threadresult_t threadresult_t;
typedef struct joinconfig_t joinconfig_t;
typedef struct join_result_t join_result_t;

struct row_t {
    type_key key;
    type_value payload;
};

struct table_t {
    struct row_t* tuples;
    uint64_t num_tuples;
    int ratio_holes;
    int sorted;
};

struct output_list_t {
    type_key key;
    type_value Rpayload;
    type_value Spayload;
    struct output_list_t * next;
};

struct output_t {
    output_list_t * list;
    uint64_t size;
};

typedef struct algorithm_t {
    char name[128];
    result_t *  (*join)(struct table_t*, struct table_t*, joinconfig_t *);
} algorithm_t;

/** Holds the join results of a thread */
struct threadresult_t {
    uint64_t  nresults;
    output_list_t *   results;
    uint32_t threadid;
};

/** Type definition for join results. */
struct result_t {
    uint64_t                totalresults;
    struct threadresult_t * resultlist;
    int                     nthreads;
    double                  throughput;
};

struct join_result_t {
    uint64_t matches;
    uint64_t checksum;
    uint64_t time_usec;
    uint64_t part_usec;
    uint64_t join_usec;
    uint64_t join_build_usec;
    uint64_t join_partition_usec;
    uint64_t join_probe_usec;
    uint64_t timer_ht;
    uint64_t timer_tree;
    uint64_t timer_part;
    uint64_t timer_probe;
    uint64_t ** timer_per_partition;
#ifdef COUNT_SCANNED_TUPLES
    uint64_t scanned_tuples;
#endif
};

struct timers_t {
    uint64_t total, timer1, timer2, timer3, timer4;
    uint64_t start, end;
};

struct rusage_reduced_t {
    signed long ru_utime_sec;
    signed long ru_utime_usec;
    signed long ru_stime_sec;
    signed long ru_stime_usec;
    long ru_minflt;
    long ru_majflt;
    long ru_nvcsw;
    long ru_nivcsw;
};

/** Join configuration parameters. */
struct joinconfig_t {
    int NTHREADS;

    int MB_RESTRICTION;

    // for partitioning join
    int RADIXBITS;

    //
    int WRITETOFILE;

    bool MATERIALIZE;

    int PRINT;

    int CRACKING_THRESHOLD;
};

typedef struct __attribute__((aligned(64))) {
    unsigned int thread_num;
    unsigned long num_elements;
    unsigned long int idx_i;
    unsigned long* histogram;
    tuple_t* data;
} chunk;

typedef struct __attribute__((aligned(64))) {
    unsigned int thread_num;
    long long int start;
    long long int end;
    long long int idx_i;
    long long int idx_j;
} info_per_thread;

#ifndef MAXLOADFACTOR
#define MAXLOADFACTOR 1.1
#endif

#endif //DATA_TYPES_H
