#ifndef TEEBENCH_H
#define TEEBENCH_H

#ifdef NATIVE_COMPILATION
#include "native_ocalls.h"
#include "pcm_commons.h"
#include "Logger.h"
#else
#include "Enclave.h"
#include "Enclave_t.h"
#endif

#endif //TEEBENCH_H
