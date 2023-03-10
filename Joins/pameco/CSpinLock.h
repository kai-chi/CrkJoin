#ifndef PMC_CSPINLOCK_H
#define PMC_CSPINLOCK_H

#include <atomic>

namespace PMC {
    class CSpinlock
    {
    public:
        void lock()
        {
            while (lck.test_and_set(std::memory_order_acquire))
            {
            }
        }

        void unlock()
        {
            lck.clear(std::memory_order_release);
        }

    private:
        std::atomic_flag lck = ATOMIC_FLAG_INIT;
    };
}

#endif