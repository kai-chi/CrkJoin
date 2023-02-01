#ifndef TPCH_COMMONS_HPP
#define TPCH_COMMONS_HPP

#include "data-types.h"
#include "TpcHTypes.hpp"
#include <string>

struct tcph_args_t {
//    algorithm_t *algorithm;
    char algorithm_name[128];
    uint8_t query;
    uint8_t nthreads;
    uint8_t bits;
    uint8_t scale;
    int mb_restriction;

    tcph_args_t() : algorithm_name("CrkJoin"), query(12), nthreads(4), bits(13), scale(1), mb_restriction(275) {};
};

void tpch_parse_args(int argc, char ** argv, tcph_args_t * params);

int load_lineitem_from_file(LineItemTable *l_table, uint8_t query, uint8_t scale);
void free_lineitem(LineItemTable *l_table, uint8_t query);

int load_orders_from_file(OrdersTable *o_table, uint8_t query, uint8_t scale);
void free_orders(OrdersTable *o_table, uint8_t query);

int load_customer_from_file(CustomerTable *c_table, uint8_t query, uint8_t scale);
void free_customer(CustomerTable *c_table, uint8_t query);

int load_part_from_file(PartTable *p, uint8_t query, uint8_t scale);
void free_part(PartTable *p, uint8_t query);

int load_nation_from_file(NationTable *n, uint8_t query, uint8_t scale);
void free_nation(NationTable *n, uint8_t query);

void print_query_results(uint64_t totalTime, uint64_t filterTime, uint64_t joinTime);
#endif //TPCH_COMMONS_HPP
