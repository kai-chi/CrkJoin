/*
	This code is part of the MCJoin project.
	Authored by Steven Begley (sbegley@latrobe.edu.au) as part of my PhD candidature.
	La Trobe University,
	Melbourne, Australia.
*/

#ifndef MCJ_CFLIPFLOPBUFFER_H
#define MCJ_CFLIPFLOPBUFFER_H

#pragma once

#include "util.h"
#include "CFlipFlop.h"

namespace MCJ
{
    class CFlipFlopBuffer
    {
    public:
        CFlipFlopBuffer(const relation_t* input, uint32_t radixLength, uint32_t maxDLength) :
        _input(input), _source(NULL), _destination(NULL), _radixLength(radixLength), _maxDLength(maxDLength)
        {
        }

        ~CFlipFlopBuffer()
        {
            if(_canDeleteSource)
            {
//			delete _source;
                free(_source->tuples);
            }
        }

        relation_t* doPartitioning(uint32_t startIndex, uint32_t endIndex, uint32_t baseIndex, bool retainPayload)
        {
            uint32_t D(0);
            uint32_t bitsProcessed(0);
            CFlipFlop ff;
            const relation_t* tmp(NULL);
            uint32_t itemCount(endIndex - startIndex);

            _source = _input;
            _canDeleteSource = false;

            while(bitsProcessed != _radixLength)
            {
                D = _maxDLength;
                if( (D + bitsProcessed) > _radixLength)
                {
                    D = _radixLength - bitsProcessed;
                }

                //
                if(_destination == NULL)
                {
//				_destination = new CRelation(itemCount);
                    _destination = (relation_t*) malloc(sizeof(relation_t));
                    malloc_check(_destination);
                    _destination->tuples = (row_t *) malloc(itemCount * sizeof(row_t));
                    malloc_check(_destination->tuples);
                    _destination->num_tuples = itemCount;
                }

                ff.setRelations(_source, _destination, itemCount);

                if(bitsProcessed == 0)
                {
                    ff.go(D, bitsProcessed, startIndex, endIndex, baseIndex, retainPayload);
                }
                else
                {
                    ff.go(D, bitsProcessed);
                }
                //

                bitsProcessed += D;

                if(bitsProcessed != _radixLength)
                {
                    // Switch the pointers
                    tmp = _source;
                    _source = _destination;
                    _destination = const_cast<relation_t *>(tmp);

                    if(_canDeleteSource == false)
                    {
//					_destination = new CRelation(itemCount);
                        _destination = (relation_t*) malloc(sizeof(relation_t));
                        malloc_check(_destination);
                        _destination->tuples = (tuple_t*) malloc(itemCount * sizeof(tuple_t));
                        malloc_check(_destination->tuples);
                        _destination->num_tuples = itemCount;
                        _canDeleteSource = true;
                    }
                }
            }

            return _destination;
        }

    private:
        const relation_t* _input;

        const relation_t* _source;
        relation_t* _destination;

        uint32_t _radixLength;
        uint32_t _maxDLength;

        bool _canDeleteSource;
    };
}

#endif