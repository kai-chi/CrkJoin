/*
	This code is part of the MCJoin project.
	Authored by Steven Begley (sbegley@latrobe.edu.au) as part of my PhD candidature.
	La Trobe University,
	Melbourne, Australia.
*/
#ifndef MCJ_CFLIPFLOP_H
#define MCJ_CFLIPFLOP_H

#ifdef NATIVE_COMPILATION
#include "native_ocalls.h"
#include "Logger.h"
#else
#include "Enclave.h"
#include "Enclave_t.h"
#endif

namespace MCJ
{
    class CFlipFlop
    {
    public:
        CFlipFlop(): _numberOfPartitions(0), _cardinality(0), _histogram(NULL), _input(NULL), _output(NULL)
        {
        }

        ~CFlipFlop()
        {
        }

        __attribute__((always_inline)) void setCardinality(uint32_t cardinality)
        {
            _cardinality = cardinality;
        }

        __attribute__((always_inline)) void setInput(relation_t* input)
        {
            _input = input;
        }

        __attribute__((always_inline)) void setOutput(relation_t* output)
        {
            _output = output;
        }

        __attribute__((always_inline)) void setRelations(const relation_t* input, relation_t* output, uint32_t cardinality)
        {
            _input = input;
            _output = output;

            setCardinality(cardinality);
        }

        void go(uint32_t D, uint32_t R)
        {
            buildHistogram(D, R);
            buildPrefixSum();
            scatter(D, R);
            dropHistogram();
        }

        void go(uint32_t D, uint32_t R, uint32_t start, uint32_t end, uint32_t baseIndex, bool retainPayload)
        {
            buildHistogram(D, R, start, end);
            buildPrefixSum();
            scatter(D, R, start, end, baseIndex, retainPayload);
            dropHistogram();
        }

    private:

        __attribute__((always_inline)) void dropHistogram()
        {
            if(_histogram != NULL)
            {
                free(_histogram);
                _histogram = NULL;
            }
        }

        __attribute__((always_inline)) void buildHistogram(uint32_t D, uint32_t R)
        {
            _numberOfPartitions = 1 << D;

            _histogram = (uint64_t*)calloc(_numberOfPartitions, sizeof(uint64_t));

            uint32_t M = ( ( 1 << D ) - 1) << R;
            tuple_t * p = _input->tuples;
            uint32_t idx(0);

            for(uint32_t index = 0; index < _cardinality; ++index)
            {
                idx = (p->key) & M;
                idx >>= R;

                ++(_histogram[idx]);

                ++p;
            }
        }

        __attribute__((always_inline)) void buildHistogram(uint32_t D, uint32_t R, uint32_t start, uint32_t end)
        {
            _numberOfPartitions = 1 << D;
            _histogram = (uint64_t*)calloc(_numberOfPartitions, sizeof(uint64_t));

            uint32_t M = ( ( 1 << D ) - 1) << R;
            tuple_t* p = &_input->tuples[start];

            uint32_t idx(0);
            uint32_t itemCount(end - start);

            for(uint32_t index = 0; index < itemCount; ++index)
            {
                idx = (p->key) & M;
                idx >>= R;

                ++(_histogram[idx]);

                ++p;
            }
        }

        __attribute__((always_inline)) void buildPrefixSum()
        {
            uint64_t* h = _histogram;
            uint64_t thisValue(0);
            uint64_t sum(0);

            for(uint32_t index = 0; index < _numberOfPartitions; ++index )
            {
                thisValue = *h;

                *h = sum;

                sum += thisValue;
                ++h;
            }

            uint64_t histValue(0);
            tuple_t* p = _output->tuples;
            for(uint32_t index = 0; index < _numberOfPartitions; ++index )
            {
                histValue = _histogram[index];
                _histogram[index] = (uint64_t)(p+histValue);
            }
        }

        __attribute__((always_inline)) void scatter(uint32_t D, uint32_t R)
        {
            uint32_t M = ( ( 1 << D ) - 1) << R;
            tuple_t * p = _input->tuples;
            tuple_t* write(NULL);
            uint32_t idx(0);

            for(uint32_t index = 0; index < _cardinality; ++index)
            {
                idx = (p->key) & M;
                idx >>= R;

                write = (tuple_t*)_histogram[idx];
                *write = *p;
                _histogram[idx] += sizeof(tuple_t);

                ++p;
            }
        }

        __attribute__((always_inline)) void scatter(uint32_t D, uint32_t R, uint32_t start, uint32_t end, uint32_t baseIndex, bool retainPayload)
        {
            uint32_t M = ( ( 1 << D ) - 1) << R;
            tuple_t* p = &_input->tuples[start];
            tuple_t* write(NULL);
            uint32_t idx(0);

            uint32_t itemCount(end - start);

            for(uint32_t index = 0; index < itemCount; ++index)
            {
                idx = (p->key) & M;
                idx >>= R;

                write = (tuple_t*)_histogram[idx];
                //*write = *p;
                write->key = p->key;
                write->payload = retainPayload?p->payload:((start + index) - baseIndex);

                _histogram[idx] += sizeof(tuple_t);

                ++p;
            }
        }


        void showDebug()
        {
            for(uint32_t counter = 0; counter < _numberOfPartitions; ++counter)
            {
                //TRACE2("Partition %d : [%d]\n", counter, _histogram[counter]);
// 			cout << "Partition " << counter << " : [" << _histogram[counter] << "]" << endl;
                logger(DBG, "Partition %d : [%lu]", counter, _histogram[counter]);
            }
        }


    private:
        uint32_t _numberOfPartitions;
        uint32_t _cardinality;
        uint64_t* _histogram;
        const relation_t* _input;
        relation_t* _output;
    };
}

#endif