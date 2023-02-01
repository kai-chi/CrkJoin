#ifndef PMCJOIN_H
#define PMCJOIN_H

#include "CSpinLock.h"
#include "CPMCJoin_EnhancedRProcessorSTD.h"
#include "CPMCJoin_EnhancedSProcessor.h"

namespace PMC
{
    class PMCJoin
    {
    public:
        PMCJoin(PMC::COptions *const options, SMetrics *const metrics) : _options(options), _metrics(metrics),
                                                                         _FFBs(nullptr), _partitionLocks(nullptr),
                                                                         _spinlocks(nullptr), _totalSpinlocks(0),
                                                                         _numThreads(0)
        {
            _numThreads = options->getThreads();
            uint32_t ffbSize(_options->getFlipFlopCardinality() / _numThreads);
            _FFBs = new CFlipFlopBuffer2 *[_numThreads];
            for (uint32_t counter(0); counter < _numThreads; ++counter) {
                _FFBs[counter] = new CFlipFlopBuffer2(nullptr, ffbSize, _options->getBitRadixLength(),
                                                      _options->getMaxBitsPerFlipFlopPass());
            }

            _totalSpinlocks = options->getSpinlocks();
            if (_totalSpinlocks != 0) {
                uint32_t numberOfPartitions(1 << options->getBitRadixLength());

                _spinlocks = new CSpinlock[_totalSpinlocks];
                _partitionLocks = new CSpinlock[numberOfPartitions / 1024];
            }
        }


        void doPMCJoin()
        {
            uint64_t R_relationDesiredChunkSize(0);		// Chunk sizes are in number of tuples
            uint64_t R_relationActualChunkSize(0);		// Chunk sizes are in number of tuples
            uint64_t R_relationCardinality(0);	// Number of tuples

            R_relationDesiredChunkSize = _options->getCompactHashTableCardinality();

            // Get ready to process data sets
            R_relationCardinality =  _options->getRelationR()->num_tuples;

            uint32_t R_chunkIndex(0);

            // Now we mash our way through the chunks of R and thrash the chunks of S.
            _enhancedR = new CEnhancedRProcessorSTD(_options, _metrics, _FFBs, _spinlocks, _totalSpinlocks, _partitionLocks);


            while (R_chunkIndex < R_relationCardinality)
            {
                // Bite off as big a chunk as possible...
                R_relationActualChunkSize = R_relationDesiredChunkSize;

                // ...but make sure we don't bite off more than we can chew!
                if ((R_chunkIndex + R_relationActualChunkSize) > R_relationCardinality)
                {
                    R_relationActualChunkSize = R_relationCardinality - R_chunkIndex;
                }

                // Partition up this chunk of R
                doPartitioningMT(R_chunkIndex, R_relationActualChunkSize);

                //_storageR->showDebug("R");
                R_chunkIndex += (uint32_t)R_relationActualChunkSize;

                // Update number of R chunks processed for metrics
                _metrics->r_chunksProcessed += 1;
            }

            delete _enhancedR;

            // Subtract time spent scattering from time spent flip-flopping R (it was included elsewhere)
            _metrics->time_flipFlop_r -= _metrics->time_scatter;
        }

        ~PMCJoin()
        {
            // Delete FFBs
            if (_FFBs != nullptr) {
                for (uint32_t counter(0); counter < _numThreads; ++counter) {
                    delete _FFBs[counter];
                    _FFBs[counter] = nullptr;
                }

                _FFBs = nullptr;
            }

            // Delete Spinlocks
            if (_spinlocks != nullptr) {
                delete[] _spinlocks;
                _spinlocks = nullptr;
            }

            if (_partitionLocks != nullptr) {
                delete[] _partitionLocks;
                _partitionLocks = nullptr;
            }
        }

    private:
        CEnhancedRProcessorSTD *_enhancedR;
        COptions *_options;
        SMetrics *const _metrics;
        CFlipFlopBuffer2 **_FFBs;

        CSpinlock *_partitionLocks;
        CSpinlock *_spinlocks;
        uint32_t _totalSpinlocks;

        uint32_t _numThreads;

        void doPartitioningMT(uint64_t R_chunkIndex, uint64_t R_relationActualChunkSize)
        {
            uint32_t highestBucketCount(0);
            PMC::Timer*  timerCoarse = new Timer();

//            cerr << "Building... ";
//            cerr.flush();

            timerCoarse->update();

            //_enhancedR->resetPPMBuffers(R_relationActualChunkSize);
            highestBucketCount = _enhancedR->scatterToCompactHashTable((uint32_t)R_chunkIndex, (uint32_t)(R_chunkIndex + R_relationActualChunkSize));

            timerCoarse->update();
            _metrics->time_coarse_build += timerCoarse->getElapsedTime();

            // ->>>> WE THRASH RELATION S IN HERE <<<<-
            logger(WARN, "Probing... ");
//            cerr.flush();
            CEnhancedSProcessor* esp = new CEnhancedSProcessor(_options, _enhancedR, _metrics, _FFBs);
            esp->setBucketCacheSize(highestBucketCount);
            esp->doEnhancedProcessingOfS(R_chunkIndex);
            delete esp;

            // ->>>> NO MORE THRASHING, IT MAKES ME SICK! <<<<-
            timerCoarse->update();
            _metrics->time_coarse_probe += timerCoarse->getElapsedTime();
//
            delete timerCoarse;
        }

        uint64_t memNeededInBytes(const uint32_t count, const uint32_t bitsToStore)
        {
            uint64_t result(0);

            uint64_t totalBits = count;
            totalBits *= bitsToStore;

            result = totalBits >> 6;

            if(((totalBits) & 63) != 0)
            {
                ++result;	// Round up to next 64-bit boundary
            }

            result <<= 3;

            return result;
        }

        uint32_t getBitsNeededForOffsetRepresentation(uint32_t itemCount)
        {
            double bits = log((double)itemCount) / log(2.0);

            return (uint32_t)ceil(bits);
        }
    };
}
#endif //PMCJOIN_H
