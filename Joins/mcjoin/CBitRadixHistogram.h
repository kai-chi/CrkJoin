/*
	This code is part of the MCJoin project.
	Authored by Steven Begley (sbegley@latrobe.edu.au) as part of my PhD candidature.
	La Trobe University,
	Melbourne, Australia.
*/

#ifndef MCJ_CBITRADIXHISTOGRAM_H
#define MCJ_CBITRADIXHISTOGRAM_H

#pragma once

#include <stdio.h>
#ifdef NATIVE_COMPILATION
#include <memory.h>
#endif
#include <bitset>
#include "util.h"

using namespace std;

namespace MCJ
{
    class CBitRadixHistogram
    {
    public:
        CBitRadixHistogram(const uint32_t radixLength) : _radixLength(radixLength)
        {
            _numberOfPartitions = (1 << radixLength);
            _histogram = (uint32_t *)malloc(_numberOfPartitions * sizeof(uint32_t));
            malloc_check(_histogram)
            reset();

            _radixMask = (1 << (_radixLength)) -  1;
        }

        ~CBitRadixHistogram()
        {
            free(_histogram);
        }

        __attribute__((always_inline)) uint32_t getRadixLength()
        {
            return _radixLength;
        }

        inline void reset()
        {
            memset(_histogram, 0, _numberOfPartitions * sizeof(uint32_t));
            _itemCount = 0;
        }

/*
	*** If this remains commented: delete it! ***

	inline void buildHistogramPayload(const CRelation* r, const uint32_t from, const uint32_t to)
	{
		uint32_t recordCount(to - from);
		SBUN* p = &(r->getBATPointer()[from]);

		_itemCount = recordCount;

		for(uint32_t counter = 0; counter < recordCount; ++counter)
		{
			addValueToHistogram(p->payload);

			++p;
		}
	}
*/

        void buildHistogramKey(const relation_t* r, const uint32_t from, const uint32_t to)
        {
            uint32_t recordCount(to - from);
            tuple_t* p = &(r->tuples[from]);

            _itemCount = recordCount;

            for(uint32_t counter = 0; counter < recordCount; ++counter)
            {
                addValueToHistogram(p->key);

                ++p;
            }
        }

        inline void buildPrefixSum()
        {
            uint32_t* h = _histogram;
            uint32_t thisValue(0);
            uint32_t sum(0);

            for(uint32_t index = 0; index < _numberOfPartitions; ++index )
            {
                thisValue = *h;

                *h = sum;

                sum += thisValue;
                ++h;
            }
        }

        inline void rollbackPrefixSum()
        {
            uint32_t* h = _histogram;
            uint32_t thisValue(0);
            uint32_t lastValue(0);

            for(uint32_t index = 0; index < _numberOfPartitions; ++index )
            {
                thisValue = *h;

                *h = lastValue;

                ++h;

                lastValue = thisValue;
            }
        }


        __attribute__((always_inline)) void addValueToHistogram(const uint32_t value)
        {
            uint32_t maskedValue = value & _radixMask;

            ++(_histogram[maskedValue]);
        }


        __attribute__((always_inline)) uint32_t getNumberOfPartitions()
        {
            return _numberOfPartitions;
        }

        inline uint32_t getItemCount()
        {
            return _itemCount;
        }

        inline uint32_t getItemCountPostSum(uint32_t partition)
        {
            // To calculate how many items exist in this partiton, post prefix-sum operation,
            // I subtract the value at the supplied partition from the next one in sequence.
            // If the partition nominated is the END partition, I subtract the value of that one
            // from the total number of items stored in this histogram.

            uint32_t nextValue(0);

            if(partition < (_numberOfPartitions - 1))
            {
                nextValue = _histogram[partition + 1];
            }
            else
            {
                nextValue = _itemCount;
            }

            uint32_t result(nextValue - _histogram[partition]);

            return result;
        }

        inline size_t getHistogramMemoryAllocated()
        {
            return _numberOfPartitions * sizeof(uint32_t);
        }

        inline uint32_t& operator[] (const int index)
        {
            return _histogram[index];
        }

        __attribute__((always_inline)) uint32_t* getHistogramArray()
        {
            return _histogram;
        }


    private:

        uint32_t _radixLength;
        uint32_t _radixMask;
        uint32_t _numberOfPartitions;
        uint32_t _itemCount;
        uint32_t* _histogram;
    };
}

#endif