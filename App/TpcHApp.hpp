#ifndef TPCHAPP_HPP
#define TPCHAPP_HPP

#include "sgx_error.h"       /* sgx_status_t */
#include "sgx_eid.h"     /* sgx_enclave_id_t */
#include <string>

#ifndef TRUE
# define TRUE 1
#endif

#ifndef FALSE
# define FALSE 0
#endif

# define TOKEN_FILENAME   "enclave.token"
# define ENCLAVE_FILENAME "enclave.signed.so"

const std::string LINEITEM_TBL = "lineitem.tbl";
const std::string ORDERS_TBL   = "orders.tbl";
const std::string CUSTOMER_TBL = "customer.tbl";
const std::string PART_TBL     = "part.tbl";
const std::string NATION_TBL   = "nation.tbl";

extern sgx_enclave_id_t global_eid;

#if defined(__cplusplus)
extern "C" {
#endif

#if defined(__cplusplus)
}
#endif



#endif //TPCHAPP_HPP
