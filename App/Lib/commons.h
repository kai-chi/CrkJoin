#ifndef _COMMONS_H_
#define _COMMONS_H_

#include "data-types.h"

typedef struct args_t {
    algorithm_t* algorithm;
    char algorithm_name[128];
    char r_path[512];
    char s_path[512];
    uint64_t r_size;
    uint64_t s_size;
    uint32_t r_seed;
    uint32_t s_seed;
    uint32_t nthreads;
    uint32_t selectivity;
    double skew;
    int seal;
    uint32_t seal_chunk_size; // seal chunk size in kBs
    int sort_r;
    int sort_s;
    int r_from_path;
    int s_from_path;
    int three_way_join;
    int bits;
    char experiment_name[256];
    int write_to_file;
    int mb_restriction;
    int number_of_queries;
    int query_delay_ms;
    int crackingThreshold;
} args_t;

void set_default_args(args_t * params);

void set_joinconfig(joinconfig_t * jc, args_t * args);

void parse_args(int argc, char ** argv, args_t * params, struct algorithm_t algorithms[]);

void print_relation(relation_t *rel, uint32_t num, uint32_t offset);

#endif // _COMMONS_H_
