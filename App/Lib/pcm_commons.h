#ifndef SGXJOINEVALUATION_PCM_COMMONS_H
#define SGXJOINEVALUATION_PCM_COMMONS_H

#ifdef PCM_COUNT
#include "cpucounters.h"
#endif

#include <sys/resource.h>
#include "data-types.h"


typedef struct PCMCustomEvent PCMCustomEvent;

struct PCMCustomEvent {
    char name[64];
    int32_t event_number;
    int32_t umask_value;
};

extern PCMCustomEvent customEvents[2];

extern int useCustomEvents;

#ifdef PCM_COUNT
void initPCM();
void ensurePmuNotBusy(PCM *m, bool forcedProgramming);
#endif

#ifdef NATIVE_COMPILATION
void ocall_set_system_counter_state(const char *message);
void ocall_get_system_counter_state(const char *message, int start_to_end);
void ocall_getrusage(rusage_reduced_t* usage_red, int print);
#endif

#endif //SGXJOINEVALUATION_PCM_COMMONS_H
