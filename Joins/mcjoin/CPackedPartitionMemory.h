/*
	This code is part of the MCJoin project.
	Authored by Steven Begley (sbegley@latrobe.edu.au) as part of my PhD candidature.
	La Trobe University,
	Melbourne, Australia.
*/
#ifndef MCJ_CPACKEDPARTITIONMEMORY_H
#define MCJ_CPACKEDPARTITIONMEMORY_H

#include <math.h>
#include <cstdlib>

#include "SPageManagement.h"
#include "CBitRadixHistogram.h"

namespace MCJ
{
    class CPackedPartitionMemory
    {
    public:
        CPackedPartitionMemory(const uint32_t maxItemCount, const uint32_t dataBitRadixLength)
        {
            init(dataBitRadixLength);

            _maxItems = maxItemCount;
            resetBuffers(_maxItems);
        }

        CPackedPartitionMemory(const uint32_t dataBitRadixLength)
        {
            init(dataBitRadixLength);
        }

        __attribute__((always_inline)) void resetBuffers(const uint32_t maxItemCount)
        {
            if(_dataBuffer != NULL)
            {
                free (_dataBuffer);
            }
            if(_offsetBuffer != NULL)
            {
                free (_offsetBuffer);
            }

            // Data buffer creation
            uint64_t bytesNeeded = memNeededInBytes(maxItemCount, _dataBitsToStore);
            _totalMemoryAllocated = bytesNeeded;
            _dataBuffer = (uint64_t *)calloc((size_t)bytesNeeded, (size_t)sizeof(char));

            // Offset buffer creation
            _offsetBitsToStore = getBitsNeededForOffsetRepresentation(maxItemCount);
            bytesNeeded = memNeededInBytes(maxItemCount, _offsetBitsToStore);
            _totalMemoryAllocated += bytesNeeded;
            _offsetBuffer = (uint64_t *)calloc((size_t)bytesNeeded, (size_t)sizeof(char));
            _offsetBitMask = (1 << (_offsetBitsToStore)) -  1;
        }

        ~CPackedPartitionMemory()
        {
            if(_dataBuffer != NULL)
            {
                free(_dataBuffer);
                _dataBuffer = NULL;
            }

            if(_offsetBuffer != NULL)
            {
                free(_offsetBuffer);
                _offsetBuffer = NULL;
            }

            if(_histogram != NULL)
            {
                delete _histogram;
                _histogram = NULL;
            }
        }


        /*
        __forceinline void scatterPayload(const CRelation* r, const uint32_t from, const uint32_t to)
        {
            uint32_t partition(0);
            uint32_t recordCount(to - from);

            SBUN* p = &(r->getBATPointer()[from]);
            for(uint32_t offset = 0; offset < recordCount; ++offset)
            {
                // Work out which partition this value writes to
                partition = p->payload & _dataRadixMask;

                writeDataValue(p->payload, partition);
                writeOffsetValue(storeKeyInsteadOfOffset?p->key:offset, partition);

                // Update the number of items written to this partition
                (*_histogram)[partition]++;

                ++p;
            }
        }
        */

        __attribute__((always_inline)) void scatterKey(const relation_t* r, const uint32_t from, const uint32_t to)
        {
            uint32_t partition(0);
            uint32_t recordCount(to - from);

            row_t* p = &(r->tuples[from]);
            for(uint32_t offset = 0; offset < recordCount; ++offset)
            {
                // Work out which partition this value writes to
                partition = p->key & _dataRadixMask;

                writeDataValue(p->key, partition);
                writeOffsetValue(p->payload, partition);

                // Update the number of items written to this partition
                (*_histogram)[partition]++;

                ++p;
            }
        }

        //__forceinline void scatterOID(const CRelation* r, const uint32_t from, const uint32_t to, const bool useValuePayload)
        //{
        //	uint32_t partition(0);
        //	uint32_t recordCount(to - from);

        //	SBUN* p = &(r->getBATPointer()[from]);
        //	for(uint32_t offset = 0; offset < recordCount; ++offset)
        //	{
        //		// Work out which partition this value writes to
        //		partition = p->value & _dataRadixMask;

        //		writeDataValue(p->OID, partition);

        //		// Update the number of items written to this partition
        //		(*_histogram)[partition]++;

        //		++p;
        //	}
        //	_histogram->rollbackPrefixSum();

        //	p = &(r->getBATPointer()[from]);
        //	for(uint32_t offset = 0; offset < recordCount; ++offset)
        //	{
        //		// Work out which partition this value writes to
        //		partition = p->value & _dataRadixMask;

        //		writeOffsetValue(useValuePayload?p->value:offset, partition);

        //		// Update the number of items written to this partition
        //		(*_histogram)[partition]++;

        //		++p;
        //	}
        //	_histogram->rollbackPrefixSum();
        //}

        inline void writeOffsetValue(const uint32_t offset, const uint32_t partition)
        {
            // Work out which partition this value writes to
            //uint32_t partition = value & _dataRadixMask;

            // Write to packed buffer
            writeOffsetBits(offset, partition);

            // Update the number of items written to this partition
            //(*_histogram)[partition]++;

        }

        inline void writeDataValue(const uint32_t value, const uint32_t partition)
        {
            // Work out which partition this value writes to
            //uint32_t partition = value & _dataRadixMask;

            // Remove the radix portion of the value
            uint32_t bitsToWrite = value >> _dataBitRadixLength;

            // Write to packed buffer
            writeDataBits(bitsToWrite, partition);

            // Update the number of items written to this partition
            //(*_histogram)[partition]++;
        }


        __attribute__((always_inline)) uint32_t readOffsetValue(uint32_t partition,  uint32_t itemOffset)
        {

            uint32_t itemNumber = (*_histogram)[partition] + itemOffset;
            SPageManagement readTarget;
            readTarget.calculateOffset(itemNumber, _offsetBitsToStore);

            uint64_t readBits(0);

            if(readTarget.offset + _offsetBitsToStore <= 64)	// Can read from a single 64-bit page
            {
                readBits = _offsetBuffer[readTarget.page];
                uint32_t shift(64 - readTarget.offset - _offsetBitsToStore);
                readBits >>= shift;
                readBits &= _offsetBitMask;
            }
            else
            {
                // Read the left side
                readBits = _offsetBuffer[readTarget.page];

                uint32_t shift(readTarget.offset + _offsetBitsToStore - 64);

                readBits <<= shift;
                uint32_t leftSide = (uint32_t)readBits;

                // Read the right side
                readBits = _offsetBuffer[readTarget.page + 1];

                shift = 64 - (_offsetBitsToStore - (64 - readTarget.offset));

                readBits >>= shift;	// Should crunch out any superfluous data at the end of the bits of interest
                uint32_t rightSide = (uint32_t)readBits;

                readBits = leftSide | rightSide;

                readBits &= _offsetBitMask;
            }

            return (uint32_t)readBits;
        }


        __attribute__((always_inline)) uint32_t readDataValue(uint32_t partition, uint32_t itemOffset)
        {
            uint32_t itemNumber = (*_histogram)[partition] + itemOffset;
            SPageManagement readTarget;
            readTarget.calculateOffset(itemNumber, _dataBitsToStore);

            uint64_t readBits(0);

            if(readTarget.offset + _dataBitsToStore <= 64)	// Can read from a single 64-bit page
            {
                readBits = _dataBuffer[readTarget.page];
                uint32_t shift(64 - readTarget.offset - _dataBitsToStore);
                readBits >>= shift;
                readBits <<= _dataBitRadixLength;

                readBits |= partition;
            }
            else
            {
                // Read the left side
                readBits = _dataBuffer[readTarget.page];

                uint32_t shift(readTarget.offset + _dataBitsToStore - 64);

                readBits <<= (shift + _dataBitRadixLength);
                uint32_t leftSide = (uint32_t)readBits;

                // Read the right side
                readBits = _dataBuffer[readTarget.page + 1];

                shift = 64 - (_dataBitsToStore - (64 - readTarget.offset));

                readBits >>= shift;	// Should crunch out any superfluous data at the end of the bits of interest
                readBits <<= _dataBitRadixLength;
                uint32_t rightSide = (uint32_t)readBits;

                readBits = leftSide | rightSide;
                readBits |= partition;
            }

            return (uint32_t)readBits;
        }

        __attribute__((always_inline)) void writeOffsetBits(const uint32_t bits, const uint32_t partition)
        {
            uint32_t itemNumber = (*_histogram)[partition];
            SPageManagement writeTarget;
            writeTarget.calculateOffset(itemNumber, _offsetBitsToStore);

            uint64_t writeBits(bits);

            if((writeTarget.offset + _offsetBitsToStore) <= 64)	// can fit into single 64-bit page
            {
                uint32_t shift(64 - writeTarget.offset - _offsetBitsToStore);

                writeBits <<= shift;

                uint64_t& target = _offsetBuffer[writeTarget.page];

                target |= writeBits;
            }
            else										// spans two pages
            {

                // Write the left side
                uint32_t shift(_offsetBitsToStore - (64 - writeTarget.offset));
                writeBits >>= shift;

                uint64_t& leftTarget = _offsetBuffer[writeTarget.page];
                leftTarget |= writeBits;

                // Write the right ride
                shift = (64 - _offsetBitsToStore + (_offsetBitsToStore - shift));
                writeBits = bits;
                writeBits <<= shift;

                uint64_t& rightTarget = _offsetBuffer[writeTarget.page + 1];
                rightTarget |= writeBits;
            }
        }

        __attribute__((always_inline)) void writeDataBits(const uint32_t bits, const uint32_t partition)
        {
            uint32_t itemNumber = (*_histogram)[partition];
            SPageManagement writeTarget;
            writeTarget.calculateOffset(itemNumber, _dataBitsToStore);

            uint64_t writeBits(bits);

            if((writeTarget.offset + _dataBitsToStore) <= 64)	// can fit into single 64-bit page
            {
                uint32_t shift(64 - writeTarget.offset - _dataBitsToStore);

                writeBits <<= shift;

                uint64_t& target = _dataBuffer[writeTarget.page];

                target |= writeBits;
            }
            else										// spans two pages
            {

                // Write the left side
                uint32_t shift(_dataBitsToStore - (64 - writeTarget.offset));
                writeBits >>= shift;

                uint64_t& leftTarget = _dataBuffer[writeTarget.page];
                leftTarget |= writeBits;

                // Write the right ride
                shift = (64 - _dataBitsToStore + (_dataBitsToStore - shift));
                writeBits = bits;
                writeBits <<= shift;

                uint64_t& rightTarget = _dataBuffer[writeTarget.page + 1];
                rightTarget |= writeBits;
            }
        }

        __attribute__((always_inline)) CBitRadixHistogram* getHistogram()
        {
            return _histogram;
        }

        void showDebug()
        {
            uint32_t key(0);
            uint32_t payload(0);
            uint32_t partitionCount = _histogram->getNumberOfPartitions();

            for(uint32_t partitionCounter = 0; partitionCounter < partitionCount; ++partitionCounter )
            {
                uint32_t itemCount = _histogram->getItemCountPostSum(partitionCounter);
                for(uint32_t itemIndex = 0; itemIndex < itemCount; ++itemIndex)
                {
                    key = readDataValue(partitionCounter, itemIndex);
                    payload = readOffsetValue(partitionCounter, itemIndex);

                    printf("Partition: %d, Key: %d, Payload: %d\n", partitionCounter, key, payload);
                }
            }
        }

        uint32_t getBitsNeededForOffsetRepresentation(uint32_t itemCount)
        {
            double bits = log((double)itemCount) / log(2.0);

            return (uint32_t)ceil(bits);
        }

        uint64_t memNeededInBytes(const uint32_t count, const uint32_t bitsToStore)
        {
            uint64_t result(0);

            //result = (count * bitsToStore / 64);
            uint64_t totalBits = count;
            totalBits *= bitsToStore;

            //result = totalBits / 64;
            result = totalBits >> 6;

            //if((totalBits) % 64 != 0)
            if(((totalBits) & 63) != 0)
            {
                ++result;	// Round up to next 64-bit boundary
            }

            //result *= 8;
            result <<= 3;

            return result;
        }

        inline uint64_t getTotalMemoryAllocated()
        {
            return _totalMemoryAllocated;
        }

    private:
        void init(uint32_t dataBitRadixLength)
        {
            _maxItems = 0;
            _dataBuffer = NULL;
            _dataBitRadixLength = dataBitRadixLength;
            _dataBitsToStore = (32 - dataBitRadixLength);
            _dataRadixMask = 0;
            _offsetBuffer = NULL;
            _offsetBitsToStore = 0;

            // Histogram and radix mask
            _histogram = new CBitRadixHistogram(dataBitRadixLength);
            _dataRadixMask = (1 << (_dataBitRadixLength)) -  1;
            _offsetBitMask = (1 << (_offsetBitsToStore)) -  1;
        }

    private:
        uint64_t* _dataBuffer;
        uint32_t _maxItems;
        uint32_t _dataBitRadixLength;
        uint32_t _dataBitsToStore;
        uint32_t _dataRadixMask;

        uint64_t* _offsetBuffer;
        uint32_t _offsetBitsToStore;
        uint32_t _offsetBitMask;

        CBitRadixHistogram* _histogram;

        uint64_t _totalMemoryAllocated;
    };
}

#endif