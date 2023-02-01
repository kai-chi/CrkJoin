#include "TpcHApp.hpp"
#include "TpcH/TpcHCommons.hpp"
#include "Lib/ErrorSupport.h"
#include "sgx_urts.h"
#include "Logger.h"
#include "Enclave_u.h"

sgx_enclave_id_t global_eid = 0;
char experiment_filename [512];
int write_to_file = 0;

int initialize_enclave(void)
{
    sgx_status_t ret = SGX_ERROR_UNEXPECTED;

    logger(INFO, "Initialize enclave");
    /* Call sgx_create_enclave to initialize an enclave instance */
    /* Debug Support: set 2nd parameter to 1 */
    ret = sgx_create_enclave(ENCLAVE_FILENAME, SGX_DEBUG_FLAG, NULL, NULL, &global_eid, NULL);
    if (ret != SGX_SUCCESS) {
        ret_error_support(ret);
        return -1;
    }
    logger(INFO, "Enclave id = %d", global_eid);
    return 0;
}

int SGX_CDECL main(int argc, char *argv[])
{
    initLogger();
    joinconfig_t joinconfig;
    logger(INFO, "************* TPC-H APP *************");
    // 1. Parse args
    tcph_args_t params;
    tpch_parse_args(argc, argv, &params);
    uint8_t query = params.query;
    joinconfig.NTHREADS = (int) params.nthreads;
    joinconfig.RADIXBITS = params.bits;
    joinconfig.MB_RESTRICTION = params.mb_restriction;
    logger(INFO, "Run Q%d (scale %d) with join algorithm %s (%d threads)",
           query, params.scale, params.algorithm_name, joinconfig.NTHREADS);

    // 2. load required TPC-H tables
    LineItemTable l;
    OrdersTable o;
    CustomerTable c;
    PartTable p;
    NationTable n;

    load_orders_from_file(&o, query, params.scale);
    load_customer_from_file(&c, query, params.scale);
    load_part_from_file(&p, query, params.scale);
    load_nation_from_file(&n, query, params.scale);
    load_lineitem_from_file(&l, query, params.scale);

    // 3. init enclave
    initialize_enclave();

    // 4. execute specified query using an ECALL
    sgx_status_t ret = SGX_SUCCESS, retval;
    result_t result{};
    switch(query) {
        case 3:
            ret = ecall_tpch_q3(global_eid,
                                &retval,
                                &result,
                                &c,
                                &o,
                                &l,
                                params.algorithm_name,
                                &joinconfig);
            break;
        case 10:
            ret = ecall_tpch_q10(global_eid,
                                 &retval,
                                 &result,
                                 &c,
                                 &o,
                                 &l,
                                 &n,
                                 params.algorithm_name,
                                 &joinconfig);
            break;
        case 12:
            ret = ecall_tpch_q12(global_eid,
                                 &retval,
                                 &result,
                                 &l,
                                 &o,
                                 params.algorithm_name,
                                 &joinconfig);
            break;
        case 19:
            ret = ecall_tpch_q19(global_eid,
                                 &retval,
                                 &result,
                                 &l,
                                 &p,
                                 params.algorithm_name,
                                 &joinconfig);
            break;
        default:
            logger(ERROR, "TPC-H Q%d is not supported", query);
    }
    // 4. report and clean up
    if (ret != SGX_SUCCESS) {
        ret_error_support(ret);
    } else {
        logger(INFO, "Query completed");
    }

    free_orders(&o, query);
    free_part(&p, query);
    free_customer(&c, query);
    free_lineitem(&l, query);
    free_nation(&n, query);

    // 5. destroy enclave
    ret = sgx_destroy_enclave(global_eid);
    if (ret == SGX_SUCCESS) {
        logger(INFO, "Enclave destroyed");
    }

}