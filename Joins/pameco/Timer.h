#ifndef PMC_TIMER_H
#define PMC_TIMER_H

#include "TeeBench.h"

namespace PMC
{
    class Timer
    {

    public:
        Timer() : _elapsedTime(0)
        {
            ocall_get_system_micros(&_currentTime);
            _previousTime = _currentTime;
        }

        ~Timer()
        {
        }

        void update()
        {
            ocall_get_system_micros(&_currentTime);
            _elapsedTime = _currentTime - _previousTime;
            _previousTime = _currentTime;
        }

        uint64_t getElapsedMicros()
        {
            return _elapsedTime;
        }

        double getElapsedTime()
        {
            return (double) _elapsedTime / 1000000.0;
        }

    private:
        uint64_t _currentTime;
        uint64_t _previousTime;
        uint64_t _elapsedTime;
    };
}

#endif