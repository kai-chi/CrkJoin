#ifndef PMC_CBITRADIXHISTOGRAM_H
#define PMC_CBITRADIXHISTOGRAM_H

#include <stdio.h>
#include <bitset>
#ifdef NATIVE_COMPILATION
#include <cstring>
#endif

using namespace std;

namespace PMC
{
    class CBitRadixHistogram
    {
    public:
        CBitRadixHistogram(const uint32_t radixLength) : _radixLength(radixLength)
        {
            _numberOfPartitions = (1 << radixLength);
            _histogram = (uint32_t *)malloc(_numberOfPartitions * sizeof(uint32_t));
            reset();

            _radixMask = (1 << (_radixLength)) -  1;
//            _radixMasks = _mm_set1_epi32(_radixMask);
        }

        ~CBitRadixHistogram()
        {
            free(_histogram);
        }

        uint32_t getRadixLength()
        {
            return _radixLength;
        }

        void reset()
        {
            memset(_histogram, 0, _numberOfPartitions * sizeof(uint32_t));
            _itemCount = 0;
        }

        void buildHistogramKey(const relation_t* r, const uint32_t from, const uint32_t to)
        {
            uint32_t recordCount(to - from);
            tuple_t* p = &(r->tuples[from]);
            uint32_t maskedValue(0);

            _itemCount = recordCount;

            for(uint32_t counter = 0; counter < recordCount; ++counter)
            {
                maskedValue = p->key & _radixMask;
                ++(_histogram[maskedValue]);

                ++p;
            }
        }

        uint32_t buildPrefixSum()
        {
            uint32_t* h = _histogram;
            uint32_t thisValue(0);
            uint32_t sum(0);

            uint32_t highest(0);

            for(uint32_t index = 0; index < _numberOfPartitions; ++index )
            {
                thisValue = *h;


                if (thisValue > highest)
                {
                    highest = thisValue;
                }


                *h = sum;

                sum += thisValue;
                ++h;
            }

            return highest;
        }

        void rollbackPrefixSum()
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


        uint32_t getNumberOfPartitions()
        {
            return _numberOfPartitions;
        }

        uint32_t getItemCount()
        {
            return _itemCount;
        }

        void addToItemCount(uint32_t value)
        {
            _itemCount += value;
        }

        uint32_t getItemCountPostSum(uint32_t partition)
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

//        void showDebug()
//        {
//            char* binValue = new char[_radixLength + 1];
//
//            for(uint32_t counter = 0; counter < _numberOfPartitions; ++counter)
//            {
//                convertIntToBinary(counter, binValue, _radixLength + 1);
//                //TRACE3("Partition %3d (%s) : [%d]\n", counter, binValue, _histogram[counter]);
//                printf("Partition %3d (%s) : [%d]\n", counter, binValue, _histogram[counter]);
//                //cout << "Partition " << counter << "(" <<
//            }
//
//            delete [] binValue;
//        }


        size_t getHistogramMemoryAllocated()
        {
            return _numberOfPartitions * sizeof(uint32_t);
        }

        uint32_t& operator[] (const int index)
        {
            return _histogram[index];
        }

        uint32_t* getHistogramArray()
        {
            return _histogram;
        }

        uint32_t getRadixMask()
        {
            return _radixMask;
        }

    private:

//        void convertIntToBinary(uint32_t val, char* p, const uint32_t length)
//        {
//            // Set last character to be \0
//            p[length - 1] = '\0';
//            for(uint32_t index = length - 2; index >= 0; --index)
//            {
//                ((val & 1) == 1)?p[index] = '1':p[index] = '0';
//                val >>= 1;
//            }
//        }

        uint32_t _radixLength;

        uint32_t _radixMask;

        uint32_t _numberOfPartitions;
        uint32_t _itemCount;
        uint32_t* _histogram;
    };
}

#endif //PMC_CBITRADIXHISTOGRAM_H