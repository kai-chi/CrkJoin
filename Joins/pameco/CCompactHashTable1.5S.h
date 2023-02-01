#ifndef PMC_CCOMPACTHASHTABLE1_5S_H
#define PMC_CCOMPACTHASHTABLE1_5S_H

#include <math.h>
#include <cstdlib>

#include "CSpinLock.h"

#include "SPageManagement.h"
#include "CBitRadixHistogram.h"
#include "CCompactHashTableHelper.h"
#include "TeeBench.h"

using namespace std;

namespace PMC
{
    class CCompactHashTable
    {
    public:

        CCompactHashTable(const uint32_t dataBitRadixLength, CSpinlock *spinlocks, uint32_t totalSpinlocks,
                          CSpinlock *partitionLocks) : _dataBuffer(nullptr), _offsetBuffer(nullptr),
                                                       _histogram(nullptr), _totalSpinlocks(totalSpinlocks),
                                                       _spinlocks(spinlocks), _dataBufferSize(0), _offsetBufferSize(0),
                                                       _wordsPerSpinlock(0), _partitionLocks(partitionLocks)
        {
            init(dataBitRadixLength);
        }


        uint64_t getProjectedMemoryCost(const uint32_t maxItemCount)
        {
            uint64_t result(0);

            result = memNeededInBytes(maxItemCount, _dataBitsToStore);
            _offsetBitsToStore = getBitsNeededForOffsetRepresentation(maxItemCount);
            result += memNeededInBytes(maxItemCount, _offsetBitsToStore);

            return result;
        }

        void resetBuffers(const uint32_t maxItemCount, const uint32_t totalChunkSize)
        {
            CCompactHashTableHelper ppmh;

            if (_dataBuffer != nullptr) {
                free(_dataBuffer);
            }
            if (_offsetBuffer != nullptr) {
                free(_offsetBuffer);
            }

            // Data buffer creation
            uint64_t bytesNeeded = memNeededInBytes(maxItemCount, _dataBitsToStore);
            _dataBufferSize = bytesNeeded;
            _totalMemoryAllocated = bytesNeeded;
            _dataBuffer = (uint64_t *) calloc(bytesNeeded, sizeof(uint64_t));

            // Offset buffer creation
            _offsetBitsToStore = ppmh.getBitsNeededForOffsetRepresentation(totalChunkSize);
            bytesNeeded = ppmh.memNeededInBytes(maxItemCount, _offsetBitsToStore);
            _offsetBufferSize = bytesNeeded;
            _totalMemoryAllocated += bytesNeeded;
            _offsetBitMask = (1 << (_offsetBitsToStore)) - 1;
            _offsetBuffer = (uint64_t *) calloc((size_t) bytesNeeded, (size_t) sizeof(uint64_t));

            if (_totalSpinlocks != 0) {
                _wordsPerSpinlock = (uint32_t) ((_dataBufferSize / _totalSpinlocks) / 8) + 1;
            }
        }

        ~CCompactHashTable()
        {
            if (_dataBuffer != nullptr) {
                free(_dataBuffer);
                _dataBuffer = nullptr;
            }

            if (_offsetBuffer != nullptr) {
                free(_offsetBuffer);
                _offsetBuffer = nullptr;
            }

            if (_histogram != nullptr) {
                delete _histogram;
                _histogram = nullptr;
            }
        }


        void scatterKey(tuple_t *p)
        {
            uint32_t partition(0);
            uint32_t tupleKey(p->key);

            uint32_t spinlockIndex(0);
            uint32_t partitionLockIndex(0);

            partition = tupleKey & _dataRadixMask;
            partitionLockIndex = partition >> 10;

            if (_totalSpinlocks != 0) {
                _partitionLocks[partitionLockIndex].lock();
            }

            spinlockIndex = writeDataValue(tupleKey, partition);
            writeOffsetValue(p->payload, partition);

            (*_histogram)[partition]++;

            if (_totalSpinlocks != 0) {
                _spinlocks[spinlockIndex].unlock();
                _partitionLocks[partitionLockIndex].unlock();
            }
        }

        void writeOffsetValue(const uint32_t offset, const uint32_t partition)
        {
            // Work out which partition this value writes to
            //uint32_t partition = value & _dataRadixMask;

            // Write to packed buffer
            writeOffsetBits(offset, partition);

            // Update the number of items written to this partition
            //(*_histogram)[partition]++;

        }

        uint32_t writeDataValue(const uint32_t value, const uint32_t partition)
        {
            // Work out which partition this value writes to
            //uint32_t partition = value & _dataRadixMask;

            // Remove the radix portion of the value
            uint32_t bitsToWrite = value >> _dataBitRadixLength;

            // Write to packed buffer
            return writeDataBits(bitsToWrite, partition);

            // Update the number of items written to this partition
            //(*_histogram)[partition]++;
        }

        uint32_t readOffsetValue(uint32_t partition, uint32_t itemOffset)
        {

            uint32_t itemNumber = (*_histogram)[partition] + itemOffset;
            SPageManagement readTarget;
            readTarget.calculateOffset(itemNumber, _offsetBitsToStore);

            uint64_t readBits(0);

            if (readTarget.offset + _offsetBitsToStore <= 64)    // Can read from a single 64-bit page
            {
                readBits = _offsetBuffer[readTarget.page];
                uint32_t shift(64 - readTarget.offset - _offsetBitsToStore);
                readBits >>= shift;
                readBits &= _offsetBitMask;
            } else {
                // Read the left side
                readBits = _offsetBuffer[readTarget.page];

                uint32_t shift(readTarget.offset + _offsetBitsToStore - 64);

                readBits <<= shift;
                uint32_t leftSide = (uint32_t) readBits;

                // Read the right side
                readBits = _offsetBuffer[readTarget.page + 1];

                shift = 64 - (_offsetBitsToStore - (64 - readTarget.offset));

                readBits >>= shift;    // Should crunch out any superfluous data at the end of the bits of interest
                uint32_t rightSide = (uint32_t) readBits;

                readBits = leftSide | rightSide;

                readBits &= _offsetBitMask;
            }

            return (uint32_t) readBits;
        }

        uint32_t readDataValue(uint32_t partition, uint32_t itemOffset)
        {
            uint32_t itemNumber = (*_histogram)[partition] + itemOffset;
            SPageManagement readTarget;
            readTarget.calculateOffset(itemNumber, _dataBitsToStore);

            uint64_t readBits(0);

            if (readTarget.offset + _dataBitsToStore <= 64)    // Can read from a single 64-bit page
            {
                readBits = _dataBuffer[readTarget.page];
                uint32_t shift(64 - readTarget.offset - _dataBitsToStore);
                readBits >>= shift;
                readBits <<= _dataBitRadixLength;
            } else {
                // Read the left side
                readBits = _dataBuffer[readTarget.page];

                uint32_t shift(readTarget.offset + _dataBitsToStore - 64);

                readBits <<= (shift + _dataBitRadixLength);
                uint32_t leftSide = (uint32_t) readBits;

                // Read the right side
                readBits = _dataBuffer[readTarget.page + 1];

                shift = 64 - (_dataBitsToStore - (64 - readTarget.offset));

                readBits >>= shift;    // Should crunch out any superfluous data at the end of the bits of interest
                readBits <<= _dataBitRadixLength;
                uint32_t rightSide = (uint32_t) readBits;

                readBits = leftSide | rightSide;
            }

            // Add partition ID to decompressed tuple
            readBits |= partition;

            return (uint32_t) readBits;
        }

        void unpackPartitionIntoCache(uint32_t partitionID, uint32_t itemCount, uint32_t *cache)
        {
            //cache->clear();

            uint32_t itemNumber = (*_histogram)[partitionID] + 0;
            uint64_t totalDataBits = (uint64_t) itemNumber * (uint64_t) _dataBitsToStore;
            SPageManagement readTarget;

            uint64_t readBits(0);

            for (uint32_t counter(0); counter < itemCount; ++counter) {
                readTarget.calculateOffset(totalDataBits);

// ****************************

                if (readTarget.offset + _dataBitsToStore <= 64)    // Can read from a single 64-bit page
                {
                    readBits = _dataBuffer[readTarget.page];
                    uint32_t shift(64 - readTarget.offset - _dataBitsToStore);
                    readBits >>= shift;
                    readBits <<= _dataBitRadixLength;
                } else {
                    // Read the left side
                    readBits = _dataBuffer[readTarget.page];

                    uint32_t shift(readTarget.offset + _dataBitsToStore - 64);

                    readBits <<= (shift + _dataBitRadixLength);
                    uint32_t leftSide = (uint32_t) readBits;

                    // Read the right side
                    readBits = _dataBuffer[readTarget.page + 1];

                    shift = 64 - (_dataBitsToStore - (64 - readTarget.offset));

                    readBits >>= shift;    // Should crunch out any superfluous data at the end of the bits of interest
                    readBits <<= _dataBitRadixLength;
                    uint32_t rightSide = (uint32_t) readBits;

                    readBits = leftSide | rightSide;
                }

                // Add partition ID to decompressed tuple
                readBits |= partitionID;

                cache[counter] = (uint32_t) readBits;
                //cache->push_back((uint32_t)readBits);

// ****************************
                totalDataBits += _dataBitsToStore;
            }
        }


        void writeOffsetBits(const uint32_t bits, const uint32_t partition)
        {
            uint32_t itemNumber = (*_histogram)[partition];
            SPageManagement writeTarget;
            writeTarget.calculateOffset(itemNumber, _offsetBitsToStore);

            uint64_t bitsToWrite(bits);

            if ((writeTarget.offset + _offsetBitsToStore) <= 64)    // can fit into single 64-bit page
            {
                uint32_t shift(64 - writeTarget.offset - _offsetBitsToStore);

                bitsToWrite <<= shift;

                uint64_t &target = _offsetBuffer[writeTarget.page];

                target |= bitsToWrite;
            } else                                        // spans two pages
            {

                // Write the left side
                uint32_t shift(_offsetBitsToStore - (64 - writeTarget.offset));
                bitsToWrite >>= shift;

                uint64_t &leftTarget = _offsetBuffer[writeTarget.page];
                leftTarget |= bitsToWrite;

                // Write the right ride
                shift = (64 - _offsetBitsToStore + (_offsetBitsToStore - shift));
                bitsToWrite = bits;
                bitsToWrite <<= shift;

                uint64_t &rightTarget = _offsetBuffer[writeTarget.page + 1];
                rightTarget |= bitsToWrite;
            }
        }

        uint32_t writeDataBits(const uint32_t bits, const uint32_t partition)
        {
            uint32_t itemNumber = (*_histogram)[partition];
            SPageManagement writeTarget;
            writeTarget.calculateOffset(itemNumber, _dataBitsToStore);
            uint32_t spinlockIndex(0);

            if (_totalSpinlocks != 0) {
                spinlockIndex = writeTarget.page / _wordsPerSpinlock;
                if (spinlockIndex >= _totalSpinlocks) {
                    logger(ERROR, "Spinlock location error!");
                    ocall_exit(-1);
                }
                _spinlocks[spinlockIndex].lock();
            }

            uint64_t writeBits(bits);

            if ((writeTarget.offset + _dataBitsToStore) <= 64)    // can fit into single 64-bit page
            {
                uint32_t shift(64 - writeTarget.offset - _dataBitsToStore);

                writeBits <<= shift;

                uint64_t &target = _dataBuffer[writeTarget.page];

                target |= writeBits;
            } else                                        // spans two pages
            {
                // Write the left side
                uint32_t shift(_dataBitsToStore - (64 - writeTarget.offset));
                writeBits >>= shift;

                uint64_t &leftTarget = _dataBuffer[writeTarget.page];
                leftTarget |= writeBits;

                // Write the right ride
                shift = (64 - _dataBitsToStore + (_dataBitsToStore - shift));
                writeBits = bits;
                writeBits <<= shift;

                uint64_t &rightTarget = _dataBuffer[writeTarget.page + 1];
                rightTarget |= writeBits;
            }

            return spinlockIndex;
        }

        CBitRadixHistogram *getHistogram()
        {
            return _histogram;
        }

        void showDebug()
        {
            uint32_t key(0);
            uint32_t payload(0);
            uint32_t partitionCount = _histogram->getNumberOfPartitions();

            for (uint32_t partitionCounter = 0; partitionCounter < partitionCount; ++partitionCounter) {
                uint32_t itemCount = _histogram->getItemCountPostSum(partitionCounter);
                for (uint32_t itemIndex = 0; itemIndex < itemCount; ++itemIndex) {
                    key = readDataValue(partitionCounter, itemIndex);
                    payload = readOffsetValue(partitionCounter, itemIndex);

                    printf("Bucket: %d, Key: %d, Payload: %d\n", partitionCounter, key, payload);
                }
            }
        }

        uint32_t getBitsNeededForOffsetRepresentation(uint32_t itemCount)
        {
            double bits = log((double) itemCount) / log(2.0);

            return (uint32_t) ceil(bits);
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
            if (((totalBits) & 63) != 0) {
                ++result;    // Round up to next 64-bit boundary
            }

            //result *= 8;
            result <<= 3;

            return result;
        }

        uint64_t getTotalMemoryAllocated()
        {
            return _totalMemoryAllocated;
        }

    private:
        void init(uint32_t dataBitRadixLength)
        {
            _maxItems = 0;
            _dataBuffer = nullptr;
            _dataBitRadixLength = dataBitRadixLength;
            _dataBitsToStore = (32 - dataBitRadixLength);
            _dataRadixMask = 0;
            _offsetBuffer = nullptr;
            _offsetBitsToStore = 0;

            // Histogram and radix mask
            _histogram = new CBitRadixHistogram(dataBitRadixLength);
            _dataRadixMask = (1 << (_dataBitRadixLength)) - 1;
            _offsetBitMask = (1 << (_offsetBitsToStore)) - 1;
        }

    private:
        uint64_t *_dataBuffer;
        uint32_t _maxItems;
        uint32_t _dataBitRadixLength;
        uint32_t _dataBitsToStore;
        uint32_t _dataRadixMask;

        uint64_t *_offsetBuffer;
        uint32_t _offsetBitsToStore;
        uint32_t _offsetBitMask;

        CBitRadixHistogram *_histogram;

        uint64_t _totalMemoryAllocated;
        uint64_t _totalSpinlocks;

        CSpinlock *_spinlocks;
        uint64_t _dataBufferSize;
        uint64_t _offsetBufferSize;
        uint32_t _wordsPerSpinlock;

        CSpinlock *_partitionLocks;
    };
}

#endif //PMC_CCOMPACTHASHTABLE1_5S_H