#ifndef JOINWRAPPER_HPP
#define JOINWRAPPER_HPP

#include "data-types.h"

result_t * CRKJ(relation_t *relR, relation_t *relS, joinconfig_t * config);

/* This is a single-threaded version of CRKJ */
result_t * CRKJ_st(relation_t *relR, relation_t *relS, joinconfig_t * config);

/*
 * This is a partition-only version of CRKJ. this is just for an experiment.
 * It does not do a join.
 * */
result_t * CRKJ_partition_only(relation_t *relR, relation_t *relS, joinconfig_t * config);


/* CRKJ Simple - partition everything at once and then join */
result_t * CRKJS(relation_t *relR, relation_t *relS, joinconfig_t * config);
result_t * CRKJS_st(relation_t *relR, relation_t *relS, joinconfig_t * config);

/*CRKJ Fusion - build and probe while cracking - fuse cracking and joining */
result_t * CRKJF(relation_t *relR, relation_t *relS, joinconfig_t * config);
result_t * CRKJF_st(relation_t *relR, relation_t *relS, joinconfig_t * config);

#endif //JOINWRAPPER_HPP
