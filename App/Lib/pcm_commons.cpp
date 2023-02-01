#include "pcm_commons.h"
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/time.h>
#include "Logger.h"
#include <sys/resource.h>

#ifndef NATIVE_COMPILATION
#include "Enclave_u.h"
#endif

#ifdef SGX_COUNTERS
#include "sgx_counters.h"
#endif

static bool init = false;

int useCustomEvents = 1;

PCMCustomEvent customEvents[2] = {
//        {"TLB_FLUSH.STLB_ANY", 0xBD, 0x20},
//        {"TLB_FLUSH.DTLB_THREAD", 0xBD, 0x01},
//        {"ITLB.ITLB_FLUSH", 0xAE, 0x01},
//        {"MEM_INST_RETIRED.STLB_MISS_LOADS", 0xD0, 0x11},
//        {"MEM_INST_RETIRED.STLB_MISS_STORES", 0xD0, 0x12}
        {"CYCLE_ACTIVITY.CYCLES_MEM_ANY", 0xA3, 0x10},
        {"CYCLE_ACTIVITY.STALLS_TOTAL", 0xA3, 0x04}
};

#ifdef PCM_COUNT
static SystemCounterState state, start, end;

void initPCM() {
    PCM *m = PCM::getInstance();
    if (!useCustomEvents) {
        m->program (PCM::DEFAULT_EVENTS, NULL);
    } else {
        int custom_events_num = sizeof(customEvents) / sizeof(customEvents[0]);
        PCM::CustomCoreEventDescription events[custom_events_num];
        for (int i=0; i < custom_events_num; i++) {
            events[i].event_number = customEvents[i].event_number;
            events[i].umask_value  = customEvents[i].umask_value;
        }
        m->program(PCM::CUSTOM_CORE_EVENTS, events);
    }

    ensurePmuNotBusy(m, true);
    logger(PCMLOG, "PCM Initialized");
}

void ensurePmuNotBusy(PCM *m, bool forcedProgramming) {
    PCM::ErrorCode status;

    do {
        status = m->program();
        switch (status) {
            case PCM::PMUBusy: {
                if (!forcedProgramming) {
                    std::cout << "Warning: PMU appears to be busy, do you want to reset it? (y/n)\n";
                    char answer;
                    std::cin >>answer;
                    if (answer == 'y' || answer == 'Y')
                        m->resetPMU();
                    else
                        exit(0);
                } else {
                    m->resetPMU();
                }
                break;
            }
            case PCM::Success:
                break;
            case PCM::MSRAccessDenied:
            case PCM::UnknownError:
            default:
                exit(1);
        }
    } while (status != PCM::Success);
}
#endif

void ocall_set_system_counter_state(const char *message) {
    (void) message;
#ifdef PCM_COUNT
    if (!init)
    {
        start = getSystemCounterState();
        init = true;
        logger(PCMLOG, "Init PCM Start State");
    }
    state = getSystemCounterState();
    logger(PCMLOG,"Set system counter state: %s", message);
#else
    logger(WARN, "PCM_COUNT flag not enabled");
    return;
#endif
}

#ifdef PCM_COUNT
static void print_state(const char *message, SystemCounterState old, SystemCounterState tmp)
{
    logger(PCMLOG, "=====================================================");
    logger(PCMLOG, "Get system counter state: %s", message);
    logger(PCMLOG, "Cycles [B]              : %.2lf", (double) getCycles(old, tmp)/1000000000);
    logger(PCMLOG, "Instructions per Clock  : %.4lf", getIPC(old, tmp));
    logger(PCMLOG, "Instructions retired [M]: %.2lf", (double) getInstructionsRetired(old, tmp)/1000000);
    logger(PCMLOG, "L3 cache misses [k]     : %lu", getL3CacheMisses(old, tmp)/1000);
    logger(PCMLOG, "L3 cache hit ratio      : %.2lf", getL3CacheHitRatio(old, tmp));
    logger(PCMLOG, "L2 cache misses [k]     : %lu", getL2CacheMisses(old, tmp)/1000);
    logger(PCMLOG, "L2 cache hit ratio      : %.2lf", getL2CacheHitRatio(old, tmp));
    logger(PCMLOG, "MBytes read from DRAM   : %lu", getBytesReadFromMC(old, tmp)/1024/1024);
    logger(PCMLOG, "MBytes written from DRAM: %lu", getBytesWrittenToMC(old, tmp)/1024/1024);
#ifdef SGX_COUNTERS
    logger(PCMLOG, "EWBcnt                  : %lu", get_ewb());
#endif
    logger(PCMLOG, "end==================================================");
}
#endif

void ocall_get_system_counter_state(const char *message, int start_to_end) {
    (void) message;
    (void) start_to_end;
#ifdef PCM_COUNT
    SystemCounterState tmp = getSystemCounterState();

    if (start_to_end) {
        print_state("Overall results", start, tmp);
        if (useCustomEvents) {
            int custom_events_num = sizeof(customEvents) / sizeof(customEvents[0]);
            for (int i = 0; i < custom_events_num; i++) {
            uint64_t value = getNumberOfCustomEvents(i, start, tmp);
            logger(PCMLOG, "Event-%d %s: %lld", i+1, customEvents[i].name, value);
        }
    }
    } else {
        print_state(message, state, tmp);
        if (useCustomEvents) {
            int custom_events_num = sizeof(customEvents) / sizeof(customEvents[0]);
            for (int i = 0; i < custom_events_num; i++) {
            uint64_t value = getNumberOfCustomEvents(i, state, tmp);
            logger(PCMLOG, "Event-%d %s: %lld", i+1, customEvents[i].name, value);
        }
    }
    }

    state = getSystemCounterState();
#else
    (void) init;
    logger(WARN, "PCM_COUNT flag not enabled");
    return;
#endif
}

void ocall_getrusage(rusage_reduced_t* usage_red, int print)
{
    struct rusage usage;
    getrusage(RUSAGE_SELF, &usage);
    usage_red->ru_utime_sec = usage.ru_utime.tv_sec - usage_red->ru_utime_sec;
    usage_red->ru_utime_usec = usage.ru_utime.tv_usec - usage_red->ru_utime_usec;
    usage_red->ru_stime_sec = usage.ru_stime.tv_sec - usage_red->ru_stime_sec;
    usage_red->ru_stime_usec = usage.ru_stime.tv_usec - usage_red->ru_stime_usec;
    usage_red->ru_minflt = usage.ru_minflt - usage_red->ru_minflt;
    usage_red->ru_majflt = usage.ru_majflt - usage_red->ru_majflt;
    usage_red->ru_nvcsw = usage.ru_nvcsw - usage_red->ru_nvcsw;
    usage_red->ru_nivcsw = usage.ru_nivcsw - usage_red->ru_nivcsw;
    if (print)
    {
        logger(PCMLOG, "************************** RUSAGE **************************");
        logger(PCMLOG, "user CPU time used               : %ld.%lds", usage_red->ru_utime_sec, usage_red->ru_utime_usec);
        logger(PCMLOG, "system CPU time used             : %ld.%lds", usage_red->ru_stime_sec, usage_red->ru_stime_usec);
        logger(PCMLOG, "page reclaims (soft page faults) : %lu", usage_red->ru_minflt);
        logger(PCMLOG, "page faults (hard page faults)   : %lu", usage_red->ru_majflt);
        logger(PCMLOG, "voluntary context switches       : %lu", usage_red->ru_nvcsw);
        logger(PCMLOG, "involuntary context switches     : %lu", usage_red->ru_nivcsw);
        logger(PCMLOG, "************************** RUSAGE **************************");
    }
}

