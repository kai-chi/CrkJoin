#ifndef Q12PREDICATES_HPP
#define Q12PREDICATES_HPP

inline bool q12Predicate(LineItemTable *l, uint64_t rowID)
{
    uint8_t l_shipmode = l->l_shipmode[rowID];
    uint64_t l_commitdate = l->l_commitdate[rowID];
    uint64_t l_shipdate = l->l_shipdate[rowID];
    uint64_t l_receiptdate = l->l_receiptdate[rowID];

    return ((l_shipmode == L_SHIPMODE_MAIL || l_shipmode == L_SHIPMODE_SHIP) &&
            (l_commitdate < l_receiptdate) &&
            (l_shipdate < l_commitdate) &&
            (l_receiptdate >= TIMESTAMP_1994_01_01_SECONDS) &&
            (l_receiptdate < TIMESTAMP_1995_01_01_SECONDS));
}
#endif //Q12PREDICATES_HPP
