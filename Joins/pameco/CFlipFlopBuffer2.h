#ifndef PMC_CFLIPFLOPBUFFER2_H
#define PMC_CFLIPFLOPBUFFER2_H

#include "TeeBench.h"
#ifdef NATIVE_COMPILATION
#include <cstring>
#endif

using namespace std;

namespace PMC
{
    enum class FlipFlopBufferTargetBuffer : uint32_t
    {
        Source,
        Destination,
        Buffer1,
        Buffer2,
    };

    class CFlipFlopBuffer2
    {
    public:
        CFlipFlopBuffer2(relation_t *inputRelation, uint32_t maxFlipFlopBufferCardinality, uint32_t radixBitLength,
                         uint32_t maxRadixBitsPerPass) : _inputRelation(inputRelation), _buffer1(nullptr), _buffer2(nullptr),
                                                         _FFSource(nullptr), _FFDestination(nullptr), _histogram(nullptr),
                                                         _maxFlipFlopBufferCardinality(maxFlipFlopBufferCardinality), _radixBitLength(radixBitLength),
                                                         _maxRadixBitsPerPass(maxRadixBitsPerPass),
                                                         _maxBufferPartitions(0),
                                                         _cardinality(0),
                                                         _LSBGroupByIndex(nullptr)
        {
            createBuffer(_buffer1);
            createBuffer(_buffer2);
            createHistogram();
        }

        ~CFlipFlopBuffer2()
        {
            deleteBuffer(_buffer1);
            deleteBuffer(_buffer2);
            deleteHistogram();
            deleteLSBIndex();
        }

        uint32_t *
        getLSBGroupByIndex()
        {
            return _LSBGroupByIndex;
        }

        tuple_t *
        buildLSBGroupByIndexUINT32(uint32_t
                                   groupByBitLength,
                                   bool doGroupBy = true)
        {
            uint32_t indexCardinality(
                    (1 << groupByBitLength) + 1);    // Over allocate to avoid using conditional statements later
            uint32_t indexMask((1 << groupByBitLength) - 1);

            // If the array doesn't exist, create it
            if (_LSBGroupByIndex == nullptr) {
                _LSBGroupByIndex = new uint32_t[indexCardinality];
            }

            // Nuke the contents
            for (uint32_t counter(0); counter < indexCardinality; ++counter) {
                _LSBGroupByIndex[counter] = 0;
            }

            //cerr << "Destination buffer prior to grouping: " << endl;
            //showDebugBuffer((uint32_t*)_FFDestination);

            // group the flipflop data
            if (doGroupBy) {
                groupByUINT32(groupByBitLength);
            }

            //cerr << "Destination buffer after grouping: " << endl;
            //showDebugBuffer((uint32_t*)_FFDestination);

            // Update the indices
            uint32_t *p;
            p = (uint32_t *)
                    _FFDestination;
            uint32_t value(0);

            for (uint32_t counter(0); counter < _cardinality; ++counter) {
                value = *p;
                value = value & indexMask;

                _LSBGroupByIndex[value] += 1;

                ++p;
            }

            //cerr << "Viewing LSB groupBy index (prefixed): " << endl;
            //showDebugLSBGroupByIndex(_LSBGroupByIndex, indexCardinality);

            // I need to perform a prefix sum to get the correct indexes
            uint32_t thisValue(0);
            uint32_t prefixSum(0);
            for (uint32_t counter(0); counter < indexCardinality; ++counter) {
                thisValue = _LSBGroupByIndex[counter];
                _LSBGroupByIndex[counter] = prefixSum;

                prefixSum += thisValue;
            }

            //cerr << "Viewing LSB groupBy index (prefixed): " << endl;
            //showDebugLSBGroupByIndex(_LSBGroupByIndex, indexCardinality);

            return _FFDestination;
        }


//        tuple_t *
//        buildLSBGroupByIndex(uint32_t
//                             groupByBitLength)
//        {
//            uint32_t indexCardinality(
//                    (1 << groupByBitLength) + 1);    // Over allocate to avoid using conditional statements later
//            uint32_t indexMask((1 << groupByBitLength) - 1);
//
//            // If the array doesn't exist, create it
//            if (_LSBGroupByIndex == nullptr) {
//                _LSBGroupByIndex = new uint32_t[indexCardinality];
//            }
//
//            // Nuke the contents
//            for (uint32_t counter(0); counter < indexCardinality; ++counter) {
//                _LSBGroupByIndex[counter] = 0;
//            }
//
//            //cerr << "Destination buffer prior to grouping: " << endl;
//            //showDebugBuffer(_FFDestination);
//
//            // group the flipflop data
//            groupBy(groupByBitLength);
//
//            //cerr << "Destination buffer after grouping: " << endl;
//            //showDebugBuffer(_FFDestination);
//
//            // Update the indices
//            tuple_t *p;
//            p = _FFDestination;
//            uint32_t value(0);
//
//            for (uint32_t counter(0); counter < _cardinality; ++counter) {
//                value = p->key;
//                value = value & indexMask;
//
//                _LSBGroupByIndex[value] += 1;
//
//                ++p;
//            }
//
//            //cerr << "Viewing LSB groupBy index (prefixed): " << endl;
//            //showDebugLSBGroupByIndex(_LSBGroupByIndex, indexCardinality);
//
//            // I need to perform a prefix sum to get the correct indexes
//            uint32_t thisValue(0);
//            uint32_t prefixSum(0);
//            for (uint32_t counter(0); counter < indexCardinality; ++counter) {
//                thisValue = _LSBGroupByIndex[counter];
//                _LSBGroupByIndex[counter] = prefixSum;
//
//                prefixSum += thisValue;
//            }
//
//            //cerr << "Viewing LSB groupBy index (prefixed): " << endl;
//            //showDebugLSBGroupByIndex(_LSBGroupByIndex, indexCardinality);
//
//            return _FFDestination;
//        }

        /*
        tuple_t* doFakeFlipFlop(uint32_t start, uint32_t end, bool retainPayload = true, uint32_t offsetAdjustment = 0)
        {
            // This doesn't do a FlipFlop as such. It just sets a pointer and does nothing.
            _cardinality = (end - start + 1);
            _FFDestination = &_inputRelation->getBATPointer()[start];
            return _FFDestination;
        }
        */

        tuple_t
        *
        doFlipFlopForBuildingHistogram(uint32_t
                                       start,
                                       uint32_t end
        )
        {
            tuple_t *result(nullptr);
            uint32_t bitsProcessed(0);
            uint32_t bitsToProcessThisPass(0);
            uint32_t maxFlipFlopBufferCardinality(_maxFlipFlopBufferCardinality
                                                          << 1);        // UINT32s instead of tuple_ts (2 x UINT32s), so double the capacity!

            // Calculate the cardinality of the input (sub)set
            _cardinality = (end - start + 1);

            // If it is out of bounds, complain about it!
            if (_cardinality > maxFlipFlopBufferCardinality) {
                logger(ERROR, "(Histogram FF) Input relation would overflow flip flop buffer capacity!");
                ocall_exit(-1);
            } else if (_cardinality <= 0) {
                logger(ERROR, "(Histogram FF) Flip flop has nothing to do!");
                ocall_exit(-1);
            } else {
                // Do flip flopping!
                _FFSource = &_inputRelation->tuples[start]; // Get a pointer to the starting point of the input relation
                _FFDestination = _buffer1;

                while (bitsProcessed != _radixBitLength) {
                    bitsToProcessThisPass = _maxRadixBitsPerPass;
                    if ((bitsToProcessThisPass + bitsProcessed) > _radixBitLength) {
                        bitsToProcessThisPass = _radixBitLength - bitsProcessed;
                    }

                    if (bitsProcessed != 0) {
                        flipScatterSource();
                        flipScatterDestination();
                    }

                    if (bitsProcessed == 0) {
                        // On the first pass, we pull directly from the input relation
                        buildHistogram(_FFSource, _cardinality, bitsToProcessThisPass, bitsProcessed);
                    } else {
                        buildHistogramUINT32((uint32_t *)
                                                     _FFSource, _cardinality, bitsToProcessThisPass, bitsProcessed);
                    }

                    buildPrefixSumUINT32();

                    if (bitsProcessed == 0) {
                        // Go from an input relation (tuple_ts only) to UINT32s
                        scatterFromSBUNToUINT32(_FFSource, _cardinality, bitsToProcessThisPass, bitsProcessed);
                        //showDebugBuffer((uint32_t*)_FFDestination);
                    } else {
                        // We only deal with UINT32s now
                        scatterUINT32((uint32_t *)
                                              _FFSource, _cardinality, bitsToProcessThisPass, bitsProcessed);
                        //showDebugBuffer((uint32_t*)_FFDestination);
                    }


                    bitsProcessed += bitsToProcessThisPass;
                }

                result = _FFDestination;
            }

            return result;

        }

        tuple_t
        *
        doFakeFlipFlop(uint32_t
                       start,
                       uint32_t end,
                       bool retainPayload = true, uint32_t
                       offsetAdjustment = 0
        )
        {
            (void) offsetAdjustment;
            // This does the least amount of work as possible! Everything done in one pass - we just set the offsets as needed.
            tuple_t *result(nullptr);
            tuple_t *p(nullptr);

            _cardinality = (end - start + 1);

            _FFSource = &_inputRelation->tuples[start]; // Get a pointer to the starting point of the input relation
            _FFDestination = _buffer1;

            // Just memcopy everything across.
            memcpy(_FFDestination, _FFSource, sizeof(tuple_t) * _cardinality);

            if (!retainPayload) {
                p = _FFDestination;

                for (uint32_t counter(0); counter < _cardinality; ++counter) {
                    p->payload = counter;
                    ++p;
                }
            }

            result = _FFDestination;

            return result;
        }

        tuple_t
        *
        doFlipFlop(uint32_t
                   start,
                   uint32_t end,
                   bool retainPayload = true, uint32_t
                   offsetAdjustment = 0
        )
        {
            tuple_t *result(nullptr);
            uint32_t bitsProcessed(0);
            uint32_t bitsToProcessThisPass(0);

            // Calculate the cardinality of the input (sub)set
            _cardinality = (end - start + 1);

            // If it is out of bounds, complain about it!
            if (_cardinality > _maxFlipFlopBufferCardinality) {
                logger(ERROR, "Input relation would overflow flip flop buffer capacity!");
            } else if (_cardinality <= 0) {
                logger(ERROR, "Flip flop has nothing to do!");
            } else {
                // COMMENT THIS OUT UNLESS YOU ARE TESTING NON-FLIPFLOP SCATTERING!!!!
                //return doFakeFlipFlop(start, end, retainPayload, offsetAdjustment);

                // Do flip flopping!
                _FFSource = &_inputRelation->tuples[start]; // Get a pointer to the starting point of the input relation
                _FFDestination = _buffer1;

                while (bitsProcessed != _radixBitLength) {
                    bitsToProcessThisPass = _maxRadixBitsPerPass;
                    if ((bitsToProcessThisPass + bitsProcessed) > _radixBitLength) {
                        bitsToProcessThisPass = _radixBitLength - bitsProcessed;
                    }

                    if (bitsProcessed != 0) {
                        flipScatterSource();
                        flipScatterDestination();
                    }

                    buildHistogram(_FFSource, _cardinality, bitsToProcessThisPass, bitsProcessed);
                    buildPrefixSum();

                    if (bitsProcessed == 0 && !retainPayload) {
                        scatter(_FFSource, _cardinality, bitsToProcessThisPass, bitsProcessed, false, offsetAdjustment);
                    } else {
                        scatter(_FFSource, _cardinality, bitsToProcessThisPass, bitsProcessed);
                    }

                    bitsProcessed += bitsToProcessThisPass;
                }

                result = _FFDestination;
            }

            return result;
        }

//        tuple_t
//        *
//        doGroupBy(uint32_t
//                  numberOfBits)
//        {
//            flipScatterSource();
//            flipScatterDestination();
//
//            buildHistogram(_FFSource, _cardinality, numberOfBits, 0);
//            buildPrefixSum(numberOfBits);
//            scatter(_FFSource, _cardinality, numberOfBits, 0);
//
//            return _FFDestination;
//        }

        void groupByUINT32(uint32_t leastSignificantBitCount)
        {
            // This routine is meant to group already flip-flopped data into a sequence defined by a number of least significant bits.
            // I used this for multi-threaded scattering into CHT. I guess my thesis / journal paper will describe it in detail :P
            flipScatterSource();
            flipScatterDestination();
            buildHistogramUINT32((uint32_t *)
                                         _FFSource, _cardinality, leastSignificantBitCount, 0);
            buildPrefixSumUINT32();
            scatterUINT32((uint32_t *)
                                  _FFSource, _cardinality, leastSignificantBitCount, 0);
        }

//        void groupBy(uint32_t leastSignificantBitCount)
//        {
//            // This routine is meant to group already flip-flopped data into a sequence defined by a number of least significant bits.
//            // I used this for multi-threaded scattering into CHT. I guess my thesis / journal paper will describe it in detail :P
//            flipScatterSource();
//            flipScatterDestination();
//            buildHistogram(_FFSource, _cardinality, leastSignificantBitCount, 0);
//            buildPrefixSum();
//            scatter(_FFSource, _cardinality, leastSignificantBitCount, 0);
//        }

        uint32_t getCardinality()
        {
            return _cardinality;
        }

        void reset()
        {
            _cardinality = 0;
        }

        void setParams(uint32_t radixBitLength, uint32_t maxBitsPerPass)
        {
            _radixBitLength = radixBitLength;
            _maxRadixBitsPerPass = maxBitsPerPass;
        }

        void setRelation(relation_t *relation)
        {
            _inputRelation = relation;
        }

    private:
        void
        scatterUINT32(uint32_t *start, uint32_t itemCount, uint32_t maxBitsPerFlipFlopPass,
                      uint32_t bitsAlreadyProcessed)
        {
            uint32_t bitMask = ((1 << maxBitsPerFlipFlopPass) - 1) << bitsAlreadyProcessed;
            uint32_t *p = start;
            uint32_t *write(nullptr);
            uint32_t idx(0);

            for (uint32_t index = 0; index < itemCount; ++index) {
                idx = *p & bitMask;
                idx >>= bitsAlreadyProcessed;

                write = (uint32_t *)
                        _histogram[idx];
                *write = *p;

                _histogram[idx] += sizeof(uint32_t);

                ++p;
            }
        }

        void scatterFromSBUNToUINT32(tuple_t *start, uint32_t itemCount, uint32_t maxBitsPerFlipFlopPass,
                                                   uint32_t bitsAlreadyProcessed)
        {
            uint32_t bitMask = ((1 << maxBitsPerFlipFlopPass) - 1) << bitsAlreadyProcessed;
            tuple_t *p = start;
            uint32_t *write(nullptr);
            uint32_t idx(0);

            for (uint32_t index = 0; index < itemCount; ++index) {
                idx = (p->key) & bitMask;
                idx >>= bitsAlreadyProcessed;

                write = (uint32_t *)
                        _histogram[idx];
                *write = p->key;

                _histogram[idx] += sizeof(uint32_t);

                ++p;
            }
        }

        void
        scatter(tuple_t *start, uint32_t itemCount, uint32_t maxBitsPerFlipFlopPass, uint32_t bitsAlreadyProcessed,
                bool retainPayload = true, uint32_t offsetAdjustment = 0)
        {
            uint32_t bitMask = ((1 << maxBitsPerFlipFlopPass) - 1) << bitsAlreadyProcessed;
            tuple_t *p = start;
            tuple_t *write(nullptr);
            uint32_t idx(0);

            for (uint32_t index = 0; index < itemCount; ++index) {
                idx = (p->key) & bitMask;
                idx >>= bitsAlreadyProcessed;

                write = (tuple_t *)
                        _histogram[idx];
                write->key = p->key;
                write->payload = retainPayload ? p->payload : (offsetAdjustment + index);

                _histogram[idx] += sizeof(tuple_t);

                ++p;
            }
        }

        void buildHistogramUINT32(uint32_t *start, uint32_t itemCount, uint32_t maxBitsPerFlipFlopPass,
                                                uint32_t bitsAlreadyProcessed)
        {
            // Clear out the histogram
            memset(_histogram, 0, _maxBufferPartitions * sizeof(uint64_t));

            uint32_t bitMask = ((1 << maxBitsPerFlipFlopPass) - 1) << bitsAlreadyProcessed;
            uint32_t *p = start;

            uint32_t idx(0);

            for (uint32_t index = 0; index < itemCount; ++index) {
                idx = *p & bitMask;
                idx >>= bitsAlreadyProcessed;

                ++(_histogram[idx]);

                ++p;
            }
        }

        void
        buildHistogram(tuple_t *start, uint32_t itemCount, uint32_t maxBitsPerFlipFlopPass,
                       uint32_t bitsAlreadyProcessed)
        {
            // Clear out the histogram
            memset(_histogram, 0, _maxBufferPartitions * sizeof(uint64_t));

            uint32_t bitMask = ((1 << maxBitsPerFlipFlopPass) - 1) << bitsAlreadyProcessed;
            tuple_t *p = start;

            uint32_t idx(0);

            for (uint32_t index = 0; index < itemCount; ++index) {
                idx = (p->key) & bitMask;
                idx >>= bitsAlreadyProcessed;

                ++(_histogram[idx]);

                ++p;
            }
        }

        void buildPrefixSumUINT32()
        {
            uint64_t *h = _histogram;
            uint64_t thisValue(0);
            uint64_t sum(0);

            // Iterate over all histogram entries and perform a prefix sum
            for (uint32_t index = 0; index < _maxBufferPartitions; ++index) {
                thisValue = *h;

                *h = sum;

                sum += thisValue;
                ++h;
            }

            // Convert prefix sum values into destination addresses
            uint32_t *p = (uint32_t *)
                    _FFDestination;

            for (uint32_t index = 0; index < _maxBufferPartitions; ++index) {
                thisValue = _histogram[index];
                _histogram[index] = (uint64_t) (p + thisValue);
            }
        }


        void buildPrefixSum(uint32_t groupByBitLength = 0)
        {
            uint64_t *h = _histogram;
            uint64_t thisValue(0);
            uint64_t sum(0);

            // Iterate over all histogram entries and perform a prefix sum
            for (uint32_t index = 0; index < _maxBufferPartitions; ++index) {
                thisValue = *h;

                *h = sum;

                sum += thisValue;
                ++h;
            }

            if (groupByBitLength != 0) {
                uint32_t indexCardinality((1 << groupByBitLength) + 1);

                if (_LSBGroupByIndex == nullptr) {
                    _LSBGroupByIndex = new uint32_t[indexCardinality];
                }

                for (uint32_t counter(0); counter < indexCardinality; ++counter) {
                    _LSBGroupByIndex[counter] = (uint32_t)
                            _histogram[counter];
                }
            }

            // Convert prefix sum values into destination addresses
            tuple_t *p = _FFDestination;

            for (uint32_t index = 0; index < _maxBufferPartitions; ++index) {
                thisValue = _histogram[index];
                _histogram[index] = (uint64_t) (p + thisValue);
            }
        }

        void createHistogram()
        {
            _maxBufferPartitions = (1 << _maxRadixBitsPerPass);

            _histogram = (uint64_t *) calloc(_maxBufferPartitions, sizeof(uint64_t));
            //uint32_t size(_maxBufferPartitions * sizeof(uint64_t));
            //_histogram = (uint64_t*)boost::alignment::aligned_alloc(16, size);
            //memset(_histogram, 0, size);
        }

        void deleteHistogram()
        {
            if (_histogram != nullptr) {
                free(_histogram);
                //boost::alignment::aligned_free(_histogram);
                _histogram = nullptr;
            }
        }

        void createBuffer(tuple_t *&buffer)
        {
            if (buffer != nullptr) {
                deleteBuffer(buffer);
            }

            buffer = (tuple_t *)
                    calloc(_maxFlipFlopBufferCardinality, sizeof(tuple_t));

            //uint32_t size(_maxFlipFlopBufferCardinality * sizeof(tuple_t));
            //buffer = (tuple_t *)boost::alignment::aligned_alloc(16, size);
            //memset(buffer, 0, size);
        }

        void deleteBuffer(tuple_t *&buffer)
        {
            if (buffer != nullptr) {
                free(buffer);
                //boost::alignment::aligned_free(buffer);
                buffer = nullptr;
            }
        }

        void deleteLSBIndex()
        {
            if (_LSBGroupByIndex != nullptr) {
                delete[] _LSBGroupByIndex;
                _LSBGroupByIndex = nullptr;
            }

        }

        void flipScatterDestination()
        {
            (_FFDestination == _buffer1) ? _FFDestination = _buffer2 : _FFDestination = _buffer1;
        }

        void flipScatterSource()
        {
            (_FFSource == _buffer1) ? _FFSource = _buffer2 : _FFSource = _buffer1;
        }


        void convertIntToBinary(uint32_t val, char *p, const uint32_t length)
        {
            // Set last character to be \0
            p[length - 1] = '\0';
            for (uint32_t index = length - 1; index > 0; --index) {
                ((val & 1) == 1) ? p[index - 1] = '1' : p[index - 1] = '0';
                val >>= 1;
            }
        }

        const relation_t *_inputRelation;
        tuple_t *_buffer1;
        tuple_t *_buffer2;

        tuple_t *_FFSource;
        tuple_t *_FFDestination;

        uint64_t *_histogram;

        const uint32_t _maxFlipFlopBufferCardinality;
        uint32_t _radixBitLength;
        uint32_t _maxRadixBitsPerPass;
        uint32_t _maxBufferPartitions;
        uint32_t _cardinality;

        // Be careful with this. It may not be set.
        uint32_t *_LSBGroupByIndex;
    };

};

#endif //CFLIPFLOPBUFFER2_H