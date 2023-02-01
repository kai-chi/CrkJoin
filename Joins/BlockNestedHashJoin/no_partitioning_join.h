#ifndef _NO_PARTITIONING_JOIN_H_
#define _NO_PARTITIONING_JOIN_H_

#include "data-types.h"

result_t* PHT (struct table_t *relR, struct table_t *relS, joinconfig_t * config);

result_t* BHJ (relation_t *relR, relation_t *relS, joinconfig_t *config);

#endif // _NO_PARTITIONING_JOIN_H_
