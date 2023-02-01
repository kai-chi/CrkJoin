/*
	This code is part of the MCJoin project.
	Authored by Steven Begley (sbegley@latrobe.edu.au) as part of my PhD candidature.
	La Trobe University,
	Melbourne, Australia.
*/
#ifndef MCJ_COPTIONS_H
#define MCJ_COPTIONS_H

#include <iostream>
#ifdef NATIVE_COMPILATION
#include "native_ocalls.h"
#include "pcm_commons.h"
#include "Logger.h"
#else
#include "Enclave.h"
#include "Enclave_t.h"
#include <libcxx/string>
#endif

namespace MCJ
{
    class COptions
    {
    public:

        COptions() : _threads(1), _bitRadixLength(24), _maxBitsPerFlipFlopPass(8), _flipFlopCardinality(16000000),
        _packedPartitionMemoryCardinality(16000000), _outputBufferCardinality(1000), _memoryConstraintMB(0),
        _relationR(NULL), _relationS(NULL), _loadBinaryRelations(false)
        {
        }

        ~COptions()
        {
            if(_relationR != NULL)
            {
                delete _relationR;
            }
            if(_relationS != NULL)
            {
                delete _relationS;
            }
        }

        bool getLoadBinaryRelations()
        {
            return _loadBinaryRelations;
        }

        void setLoadBinaryRelations(bool value)
        {
            _loadBinaryRelations = value;
        }

        uint32_t getThreads()
        {
            return _threads;
        }

        void setThreads(uint32_t value)
        {
            _threads = value;
        }

        uint32_t getBitRadixLength()
        {
            return _bitRadixLength;
        }

        void setBitRadixLength(uint32_t value)
        {
            _bitRadixLength = value;
        }

        uint32_t getMaxBitsPerFlipFlopPass()
        {
            return _maxBitsPerFlipFlopPass;
        }

        void setMaxBitsPerFlipFlopPass(uint32_t value)
        {
            _maxBitsPerFlipFlopPass = value;
        }

        uint32_t getFlipFlopCardinality()
        {
            return _flipFlopCardinality;
        }

        void setFlipFlopCardinality(uint32_t value)
        {
            _flipFlopCardinality = value;
        }

        uint32_t getPackedPartitionMemoryCardinality()
        {
            return _packedPartitionMemoryCardinality;
        }

        void setPackedPartitionMemoryCardinality(uint32_t value)
        {
            _packedPartitionMemoryCardinality = value;
        }

        uint32_t getOutputBufferCardinality()
        {
            return _outputBufferCardinality;
        }

        void setOutputBufferCardinality(uint32_t value)
        {
            _outputBufferCardinality = value;
        }

        relation_t* getRelationR()
        {
            return _relationR;
        }

        void setRelationR(relation_t *value)
        {
            _relationR = value;
        }

        string getRelationRFilename()
        {
            return _relationRFilename;
        }

        void setRelationRFilename(string value)
        {
            _relationRFilename = value;
        }

        relation_t* getRelationS()
        {
            return _relationS;
        }

        void setRelationS(relation_t *value)
        {
            _relationS = value;
        }

        string getRelationSFilename()
        {
            return _relationSFilename;
        }

        void setRelationSFilename(string value)
        {
            _relationSFilename = value;
        }


        uint32_t getMemoryConstraintMB()
        {
            return _memoryConstraintMB;
        }

        void setMemoryConstraintMB(uint32_t value)
        {
            _memoryConstraintMB = value;
        }

    private:
        uint32_t _threads;
        uint32_t _bitRadixLength;
        uint32_t _maxBitsPerFlipFlopPass;
        uint32_t _flipFlopCardinality;
        uint32_t _packedPartitionMemoryCardinality;
        uint32_t _outputBufferCardinality;
        uint32_t _memoryConstraintMB;	// In megabytes
        string _relationRFilename;
        string _relationSFilename;
        relation_t *_relationR;
        relation_t *_relationS;
        bool _loadBinaryRelations;
    };
}

#endif