#ifndef Q10PREDICATES_HPP
#define Q10PREDICATES_HPP

inline bool q10OrdersPredicate(OrdersTable *o, uint64_t rowID)
{
    return o->o_orderdate[rowID] >= TIMESTAMP_1993_10_01_SECONDS &&
        o->o_orderdate[rowID] < TIMESTAMP_1994_01_01_SECONDS;
}
inline bool q10LineItemPredicate(LineItemTable *l, uint64_t rowID)
{
    return l->l_returnflag[rowID] == L_RETURNFLAG_R;
}

#endif //Q10PREDICATES_HPP

