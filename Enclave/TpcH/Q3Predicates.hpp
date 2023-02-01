#ifndef Q3PREDICATES_HPP
#define Q3PREDICATES_HPP

inline bool q3CustomerPredicate(CustomerTable *c, uint64_t rowID)
{
    return c->c_mktsegment[rowID] == MKT_BUILDING;
}

inline bool q3OrdersPredicate(OrdersTable *o, uint64_t rowID)
{
    return o->o_orderdate[rowID] < TIMESTAMP_1995_03_15_SECONDS;
}

inline bool q3LineitemPredicate(LineItemTable *l, uint64_t rowID)
{
    return l->l_shipdate[rowID] >= TIMESTAMP_1995_03_16_SECONDS;
}

#endif //Q3PREDICATES_HPP
