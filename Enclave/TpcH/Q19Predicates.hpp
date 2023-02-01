#ifndef Q19PREDICATES_HPP
#define Q19PREDICATES_HPP

inline bool q19LineItemPredicate(LineItemTable *l, uint64_t rowID)
{
    return (l->l_quantity[rowID] >= 1 && l->l_quantity[rowID] <= (20 + 10)) &&
           (l->l_shipmode[rowID] == L_SHIPMODE_AIR || l->l_shipmode[rowID] == L_SHIPMODE_AIR_REG) &&
            (l->l_shipinstruct[rowID] == L_SHIPINSTRUCT_DELIVER_IN_PERSON);
}

inline bool q19PartPredicate(PartTable *p, uint64_t rowID)
{
    return (p->p_brand[rowID] == P_BRAND_12 || p->p_brand[rowID] == P_BRAND_23 || p->p_brand[rowID] == P_BRAND_34) &&
           (p->p_container[rowID] == P_CONTAINER_SM_CASE || p->p_container[rowID] == P_CONTAINER_SM_BOX ||
                   p->p_container[rowID] == P_CONTAINER_SM_PACK || p->p_container[rowID] == P_CONTAINER_SM_PKG ||
                   p->p_container[rowID] == P_CONTAINER_MED_BAG || p->p_container[rowID] == P_CONTAINER_MED_BOX ||
                   p->p_container[rowID] == P_CONTAINER_MED_PKG || p->p_container[rowID] == P_CONTAINER_MED_PACK ||
                   p->p_container[rowID] == P_CONTAINER_LG_CASE || p->p_container[rowID] == P_CONTAINER_LG_BOX ||
                   p->p_container[rowID] == P_CONTAINER_LG_PACK || p->p_container[rowID] == P_CONTAINER_LG_PKG) &&
            (p->p_size[rowID] >= 1 && p->p_size[rowID] <= 15);
}

inline bool q19FinalPredicate(PartTable *p, uint64_t rowIdPart, LineItemTable *l, uint64_t rowIdLineItem)
{
    bool p1 = p->p_brand[rowIdPart] == P_BRAND_12 &&
            (p->p_container[rowIdPart] == P_CONTAINER_SM_CASE || p->p_container[rowIdPart] == P_CONTAINER_SM_BOX ||
                    p->p_container[rowIdPart] == P_CONTAINER_SM_PACK || p->p_container[rowIdPart] == P_CONTAINER_SM_PKG) &&
            (p->p_size[rowIdPart] >= 1 && p->p_size[rowIdPart] <= 5) &&
            (l->l_quantity[rowIdLineItem] >= 1 && l->l_quantity[rowIdLineItem] <= (1 + 10));

    bool p2 = p->p_brand[rowIdPart] == P_BRAND_23 &&
              (p->p_container[rowIdPart] == P_CONTAINER_MED_BAG || p->p_container[rowIdPart] == P_CONTAINER_MED_BOX ||
               p->p_container[rowIdPart] == P_CONTAINER_MED_PKG || p->p_container[rowIdPart] == P_CONTAINER_MED_PACK) &&
              (p->p_size[rowIdPart] >= 1 && p->p_size[rowIdPart] <= 10) &&
              (l->l_quantity[rowIdLineItem] >= 10 && l->l_quantity[rowIdLineItem] <= (10 + 10));

    bool p3 = p->p_brand[rowIdPart] == P_BRAND_34 &&
              (p->p_container[rowIdPart] == P_CONTAINER_LG_CASE || p->p_container[rowIdPart] == P_CONTAINER_LG_BOX ||
               p->p_container[rowIdPart] == P_CONTAINER_LG_PACK || p->p_container[rowIdPart] == P_CONTAINER_LG_PKG) &&
              (p->p_size[rowIdPart] >= 1 && p->p_size[rowIdPart] <= 15) &&
              (l->l_quantity[rowIdLineItem] >= 20 && l->l_quantity[rowIdLineItem] <= (20 + 10));

    return p1 || p2 || p3;
}
#endif //Q19PREDICATES_HPP
