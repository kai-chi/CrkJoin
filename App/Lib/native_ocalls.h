#ifndef _NATIVE_OCALLS_H_
#define _NATIVE_OCALLS_H_

#include <stdint.h>
//#include <unistd.h>
//#include <sys/types.h>

#ifdef DEBUG
#define SGX_ASSERT(C, M) sgx_assert(C, M)
#else
#define SGX_ASSERT(C, M)
#endif

void ocall_startTimer(uint64_t* t);

void ocall_stopTimer(uint64_t* t);

void ocall_getrusage();

void ocall_get_system_micros(uint64_t* t);

void ocall_exit(int status);

void ocall_get_num_active_threads_in_numa(int *res, int numaregionid);

void ocall_get_thread_index_in_numa(int * res, int logicaltid);

void ocall_get_cpu_id(int * res, int thread_id);

void ocall_get_num_numa_regions(int * res);

void ocall_numa_thread_mark_active(int phytid);

void ocall_throw(const char* message);

void sgx_assert(int cond, const char *errorMsg);

void ocall_write_to_file(const char *str);

#endif