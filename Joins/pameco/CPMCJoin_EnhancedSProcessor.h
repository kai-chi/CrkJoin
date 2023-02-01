#ifndef PMC_CPMCJOIN_ENHANCEDSPROCESSOR_H
#define PMC_CPMCJOIN_ENHANCEDSPROCESSOR_H

#include "COptions.h"
#include "SMetrics.h"
#include "CFlipFlopBuffer2.h"
#include "CPMCJoin_EnhancedRProcessorSTD.h"
#include "CCompactHashTable1.5S.h"
#include <vector>
#include <pthread.h>
#include <thread>
#include "CSpinLock.h"
#include "Timer.h"

namespace PMC
{
    class CEnhancedSProcessor
    {
    private:
        struct SWorkerThreadOptions
        {
            uint32_t threadID;
            uint32_t numThreads;
            uint64_t threadWorkloadCardinality;
            uint32_t flipFlopCardinality;
            uint64_t R_chunkBase;
        };

        struct SWorkerThreadOptionsStatic
        {
            uint32_t threadID;
            uint32_t numThreads;
            uint64_t threadWorkloadCardinality;
            uint32_t flipFlopCardinality;
            uint64_t R_chunkBase;
            uint32_t _bucketCacheSize;
            COptions *_options;
            CFlipFlopBuffer2 *_FFB;
            CSpinlock *_threadSpinlock;
            uint64_t *_totalJoinMatches;
            double *_totalFlipFlopTime;
            double *_totalJoinTime;
            uint32_t *_totalSChunksProcessed;
            double *_totalRestitchTime;
            uint32_t bitMask;
            CEnhancedRProcessorSTD *_vanillaR;
        };

        struct SBucketCache
        {
            int64_t bucketID;
            uint32_t *cache;

            SBucketCache() : cache(nullptr)
            {
            }

            ~SBucketCache()
            {
                if (cache != nullptr) {
                    delete[] cache;
                    cache = nullptr;
                }
            }
        };

    public:
        CEnhancedSProcessor(COptions *options, CEnhancedRProcessorSTD *vanillaR, SMetrics *metrics,
                            CFlipFlopBuffer2 **FFBs) : _vanillaR(vanillaR), _metrics(metrics), _options(options),
                                                       _FFBs(FFBs), _totalJoinMatches(0), _totalFlipFlopTime(0),
                                                       _totalJoinTime(0), _totalRestitchTime(0), _totalSChunksProcessed(0),
                                                       _bucketCacheSize(0)
        {
        }

        ~CEnhancedSProcessor()
        {
        }

        void doEnhancedProcessingOfS(uint64_t R_chunkBase)
        {
            _bitMask = (1 << _options->getBitRadixLength()) - 1;
            uint32_t _numThreads = _options->getThreads();


            for (uint32_t counter(0); counter < _numThreads; ++counter) {
                auto wto = (SWorkerThreadOptionsStatic*) malloc(sizeof(SWorkerThreadOptionsStatic));
                wto->numThreads = _numThreads;
                wto->threadWorkloadCardinality = uint64_t(ceil((double)_options->getRelationS()->num_tuples / (double) _numThreads));
                wto->flipFlopCardinality = _options->getFlipFlopCardinality() / _numThreads;
                wto->R_chunkBase = R_chunkBase;
                wto->threadID = counter;
                wto->_bucketCacheSize = _bucketCacheSize;
                wto->_options = _options;
                wto->_FFB = _FFBs[counter];
                wto->_threadSpinlock = &_threadSpinlock;
                wto->_totalJoinMatches = &_totalJoinMatches;
                wto->_totalFlipFlopTime = &_totalFlipFlopTime;
                wto->_totalJoinTime = &_totalJoinTime;
                wto->_totalSChunksProcessed = &_totalSChunksProcessed;
                wto->_totalRestitchTime = &_totalRestitchTime;
                wto->bitMask = _bitMask;
                wto->_vanillaR = _vanillaR;
                pthread_t t;
//                WorkerThreadHelperStruct helper(this, wto);
//                auto *helper = (WorkerThreadHelperStruct*) malloc(sizeof(WorkerThreadHelperStruct));
//                helper->_processor = this;
//                helper->_wto = wto;
                pthread_create(&t, nullptr, &CEnhancedSProcessor::workerThread, (void*)wto);
                _threads.push_back(t);
            }

            for (auto &thread: _threads) {
                pthread_join(thread, nullptr);
            }
            _threads.clear();

            _metrics->output_cardinality += _totalJoinMatches;
            _metrics->time_flipFlop_s += (_totalFlipFlopTime / _options->getThreads());
            _metrics->time_join += (_totalJoinTime / _options->getThreads());
            _metrics->time_restitch += (_totalRestitchTime / _options->getThreads());
            _metrics->s_chunksProcessed += (_totalSChunksProcessed / _options->getThreads());
        }

        void setBucketCacheSize(uint32_t size)
        {
            _bucketCacheSize = size;
        }

        __attribute__((unused)) uint32_t getBucketCacheSize()
        {
            return _bucketCacheSize;
        }

    private:
        /*
        void restitchTuples(uint32_t R_chunkIndex, SBUN* pOutputBuffer, uint32_t outputIndex, double &restitchTime, Timer* restitchTimer)
        {

            const bool debug(false);
            SBUN* pBATPointer(nullptr);

            register uint32_t offset;

            restitchTimer->update();

            pBATPointer = _options->getRelationR()->getBATPointer();

            for (uint64_t index = 0; index < outputIndex; ++index)
            {
                offset = pOutputBuffer->key;
                pOutputBuffer->key = pBATPointer[R_chunkIndex + offset].payload; // Yes, R.payload is being stored in O.key. I want my final output to be R.payload, S.payload (stored in O.key, O.payload respectively)

                ++pOutputBuffer;
            }

            restitchTimer->update();
            restitchTime += restitchTimer->getElapsedTime();
        }
        */

        static void
        restitchTuples(uint32_t R_chunkIndex, vector<tuple_t> *outputBuffer, uint32_t outputIndex, double &restitchTime,
                       Timer *restitchTimer, COptions *_options)
        {
            (void) outputIndex;
            tuple_t *pBATPointer(nullptr);

            uint32_t offset;

            restitchTimer->update();

            pBATPointer = _options->getRelationR()->tuples;

            /*
            std::sort(outputBuffer->begin(), outputBuffer->end(), [](SBUN a, SBUN b)
            {
                return a.key < b.key;
            });
            */

            for (auto &tuple: *outputBuffer) {
                offset = tuple.key;
                tuple.key = pBATPointer[R_chunkIndex + offset].payload;
            }

            restitchTimer->update();
            restitchTime += (double) restitchTimer->getElapsedMicros() / 1000000.0;
        }

        void
        restitchTuples(uint32_t R_chunkIndex, vector<tuple_t> *outputBuffer, uint32_t outputIndex, double &restitchTime,
                       Timer *restitchTimer)
        {
            (void) outputIndex;
            tuple_t *pBATPointer(nullptr);

            uint32_t offset;

            restitchTimer->update();

            pBATPointer = _options->getRelationR()->tuples;

            /*
            std::sort(outputBuffer->begin(), outputBuffer->end(), [](SBUN a, SBUN b)
            {
                return a.key < b.key;
            });
            */

            for (auto &tuple: *outputBuffer) {
                offset = tuple.key;
                tuple.key = pBATPointer[R_chunkIndex + offset].payload;
            }

            restitchTimer->update();
            restitchTime += (double) restitchTimer->getElapsedMicros() / 1000000.0;
        }

        static void
        doNLJProbing(tuple_t *ffbuffer, uint32_t ffbcardinality, vector<tuple_t> *outputBuffer, uint32_t &outputIndex,
                     uint32_t outputBufferCardinality, uint32_t &threadJoinMatches, uint32_t R_chunkBase,
                     double &restitchTime, Timer *restitchTimer, uint32_t _bitMask, CEnhancedRProcessorSTD *_vanillaR,
                     COptions *_options)
        {
            uint32_t R_bucketID(0);        // The ID of a bucket in Compact Hash Table
            uint32_t R_itemsInBucket(0);    // Number of items in a given partition of Compact Hash Table
            uint32_t R_tupleIndex(
                    0);            // A counter to iterate through tuples in a partition of Compact Hash Table
            uint32_t R_tupleKey(0);            // Key of a given tuple of R in Compact Hash Table

            tuple_t outTuple;

            for (uint32_t counter(0); counter < ffbcardinality; ++counter) {
                //cout << "K: " << buffer[counter].key << ", P: " << buffer[counter].payload << endl;

                // Determine what compact hash table bucket this tuple would map to.
                R_bucketID = ffbuffer[counter].key & _bitMask;

                // Do we have any items in this partition?
                R_itemsInBucket = _vanillaR->getCompactHashTable()->getHistogram()->getItemCountPostSum(R_bucketID);
                if (R_itemsInBucket != 0) {
                    for (R_tupleIndex = 0; R_tupleIndex < R_itemsInBucket; ++R_tupleIndex) {
                        R_tupleKey = _vanillaR->getCompactHashTable()->readDataValue(R_bucketID, R_tupleIndex);

                        if (R_tupleKey == ffbuffer[counter].key) // Do we have a match?
                        {
                            ++threadJoinMatches;

                            outTuple.key = _vanillaR->getCompactHashTable()->readOffsetValue(R_bucketID, R_tupleIndex);
                            outTuple.payload = ffbuffer[counter].payload;

                            outputBuffer->push_back(outTuple);

                            //outputBuffer[outputIndex].key = _vanillaR->getCompactHashTable()->readOffsetValue(R_partitionID, R_tupleIndex);
                            //outputBuffer[outputIndex].payload = ffbuffer[counter].payload;

                            ++outputIndex;

                            if (outputIndex == outputBufferCardinality)    // Output buffer is full
                            {
                                restitchTuples(R_chunkBase, outputBuffer, outputIndex, restitchTime, restitchTimer, _options);

                                outputIndex = 0;
                                outputBuffer->clear();
                            }

                        }
                    }
                }
            }
        }

        void
        doNLJProbing(tuple_t *ffbuffer, uint32_t ffbcardinality, vector<tuple_t> *outputBuffer, uint32_t &outputIndex,
                     uint32_t outputBufferCardinality, uint32_t &threadJoinMatches, uint32_t R_chunkBase,
                     double &restitchTime, Timer *restitchTimer)
        {
            uint32_t R_bucketID(0);        // The ID of a bucket in Compact Hash Table
            uint32_t R_itemsInBucket(0);    // Number of items in a given partition of Compact Hash Table
            uint32_t R_tupleIndex(
                    0);            // A counter to iterate through tuples in a partition of Compact Hash Table
            uint32_t R_tupleKey(0);            // Key of a given tuple of R in Compact Hash Table

            tuple_t outTuple;

            for (uint32_t counter(0); counter < ffbcardinality; ++counter) {
                //cout << "K: " << buffer[counter].key << ", P: " << buffer[counter].payload << endl;

                // Determine what compact hash table bucket this tuple would map to.
                R_bucketID = ffbuffer[counter].key & _bitMask;

                // Do we have any items in this partition?
                R_itemsInBucket = _vanillaR->getCompactHashTable()->getHistogram()->getItemCountPostSum(R_bucketID);
                if (R_itemsInBucket != 0) {
                    for (R_tupleIndex = 0; R_tupleIndex < R_itemsInBucket; ++R_tupleIndex) {
                        R_tupleKey = _vanillaR->getCompactHashTable()->readDataValue(R_bucketID, R_tupleIndex);

                        if (R_tupleKey == ffbuffer[counter].key) // Do we have a match?
                        {
                            ++threadJoinMatches;

                            outTuple.key = _vanillaR->getCompactHashTable()->readOffsetValue(R_bucketID, R_tupleIndex);
                            outTuple.payload = ffbuffer[counter].payload;

                            outputBuffer->push_back(outTuple);

                            //outputBuffer[outputIndex].key = _vanillaR->getCompactHashTable()->readOffsetValue(R_partitionID, R_tupleIndex);
                            //outputBuffer[outputIndex].payload = ffbuffer[counter].payload;

                            ++outputIndex;

                            if (outputIndex == outputBufferCardinality)    // Output buffer is full
                            {
                                restitchTuples(R_chunkBase, outputBuffer, outputIndex, restitchTime, restitchTimer);

                                outputIndex = 0;
                                outputBuffer->clear();
                            }

                        }
                    }
                }
            }
        }

        void
        doNLJProbing2(tuple_t *ffbuffer, uint32_t ffbcardinality, vector<tuple_t> *outputBuffer, uint32_t &outputIndex,
                      uint32_t outputBufferCardinality, uint32_t &threadJoinMatches, uint32_t R_chunkBase,
                      double &restitchTime, Timer *restitchTimer, SBucketCache *bucketCache)
        {
            uint32_t R_bucketID(0);        // The ID of a partition in Compact Hash Table
            uint32_t R_itemsInBucket(0);    // Number of items in a given partition of Compact Hash Table
            uint32_t R_tupleIndex(
                    0);            // A counter to iterate through tuples in a partition of Compact Hash Table
            uint32_t R_tupleKey(0);            // Key of a given tuple of R in Compact Hash Table

            tuple_t outTuple;

            for (uint32_t counter(0); counter < ffbcardinality; ++counter) {
                //cout << "K: " << buffer[counter].key << ", P: " << buffer[counter].payload << endl;

                // Determine what compact hash table bucket this tuple would map to.
                R_bucketID = ffbuffer[counter].key & _bitMask;

                // Do we have any items in this partition?
                R_itemsInBucket = _vanillaR->getCompactHashTable()->getHistogram()->getItemCountPostSum(R_bucketID);
                if (R_itemsInBucket != 0) {
                    if ((int64_t) R_bucketID != bucketCache->bucketID) {
                        bucketCache->bucketID = (int64_t) R_bucketID;
                        _vanillaR->getCompactHashTable()->unpackPartitionIntoCache(R_bucketID, R_itemsInBucket,
                                                                                   bucketCache->cache);
                    }

                    for (R_tupleIndex = 0; R_tupleIndex < R_itemsInBucket; ++R_tupleIndex) {
                        //R_tupleKey = _vanillaR->getCompactHashTable()->readDataValue(R_partitionID, R_tupleIndex);
                        R_tupleKey = bucketCache->cache[R_tupleIndex];

                        if (R_tupleKey == ffbuffer[counter].key) // Do we have a match?
                        {
                            ++threadJoinMatches;

                            outTuple.key = _vanillaR->getCompactHashTable()->readOffsetValue(R_bucketID, R_tupleIndex);
                            outTuple.payload = ffbuffer[counter].payload;

                            outputBuffer->push_back(outTuple);

                            ++outputIndex;

                            if (outputIndex == outputBufferCardinality)    // Output buffer is full
                            {
                                restitchTuples(R_chunkBase, outputBuffer, outputIndex, restitchTime, restitchTimer);

                                outputIndex = 0;
                                outputBuffer->clear();
                            }

                        }
                    }
                }
            }
        }

        static void * workerThread(void *params)
        {
            auto wto = static_cast<SWorkerThreadOptionsStatic*>(params);
            // Grab my local options
            uint32_t threadID = wto->threadID;
            uint32_t numThreads = wto->numThreads;
            uint64_t threadWorkloadCardinality = wto->threadWorkloadCardinality;
            uint32_t flipFlopCardinality = wto->flipFlopCardinality;
            uint64_t R_chunkBase = wto->R_chunkBase;

            // Other stuff
            uint32_t threadWorkBase(0);                // Current starting ordinal position of work in S.
            uint32_t threadWorkEnd(0);                // Ending ordinal position of the current work in S
            uint32_t threadActualWorkload(0);            // How much work we'd like to do per iteration
            uint32_t ffbStart(0);
            uint32_t ffbEnd(0);

            uint32_t outputIndex(0);                    // Where to write the next item of output.
            uint32_t outputBufferCardinality(0);        // Maximum number of items to write.
            uint32_t threadJoinMatches(0);

            // Thread metrics
            double metric_flipFlopTime(0);
            double metric_joinTime(0);
            double metric_restitchTime(0);
            uint32_t metric_sChunksProcessed(0);

            logger(DBG, "Start workerThread-%d", threadID);

            // Test out this cache
            SBucketCache *bucketCache = new SBucketCache();
            bucketCache->bucketID = -1;
            bucketCache->cache = new uint32_t[wto->_bucketCacheSize];


            Timer *timer = new Timer();
            Timer *restitchTimer = new Timer();

            outputBufferCardinality = wto->_options->getOutputBufferCardinality();

            //SBUN* outputBuffer = new SBUN[outputBufferCardinality];
            vector<tuple_t> *outputBuffer = new vector<tuple_t>(outputBufferCardinality);

            tuple_t *currentBuffer(nullptr);            // Pointer to the current post-flipflop buffer

            CFlipFlopBuffer2 *ffb = wto->_FFB;
            ffb->setRelation(wto->_options->getRelationS());

            //CFlipFlopBuffer2* ffb2 = new CFlipFlopBuffer2(_options->getRelationS(), flipFlopCardinality, _options->getBitRadixLength(), _options->getMaxBitsPerFlipFlopPass());
            //CFlipFlopBuffer2* ffb2 = new CFlipFlopBuffer2(_options->getRelationS(), flipFlopCardinality, 1, _options->getMaxBitsPerFlipFlopPass());		// Disable flip-flop to measure effect

            // Calculate the starting point in S of the current workload
            threadWorkBase = threadID * (uint32_t)threadWorkloadCardinality;

            while (threadWorkBase < wto->_options->getRelationS()->num_tuples)    // While there is still work to do...
            {
                threadActualWorkload = (uint32_t)threadWorkloadCardinality;    // The ideal workload
                if ((threadWorkBase + threadActualWorkload) >
                    wto->_options->getRelationS()->num_tuples)    // Too much work to do?
                {
                    threadActualWorkload =
                            (uint32_t)(wto->_options->getRelationS()->num_tuples - threadWorkBase);    // Lighten the workload
                }
                threadWorkEnd = threadWorkBase + threadActualWorkload - 1;

                // Now we do something similar for each flipFlop pass
                ffbStart = threadWorkBase;

                while (ffbStart <= threadWorkEnd) {
                    ffbEnd = ffbStart + flipFlopCardinality - 1;
                    if (ffbEnd > threadWorkEnd) {
                        ffbEnd = threadWorkEnd;
                    }

                    // Let's do it!
                    timer->update();
                    currentBuffer = ffb->doFlipFlop(ffbStart, ffbEnd, true);
                    timer->update();
                    metric_flipFlopTime += (double) timer->getElapsedMicros() / 1000000.0;

                    timer->update();

                    //__itt_resume();
                    doNLJProbing(currentBuffer, ffb->getCardinality(), outputBuffer, outputIndex,
                                 outputBufferCardinality, threadJoinMatches, (uint32_t)R_chunkBase, metric_restitchTime,
                                 restitchTimer, wto->bitMask, wto->_vanillaR, wto->_options);
                    //doNLJProbing2(currentBuffer, ffb->getCardinality(), outputBuffer, outputIndex, outputBufferCardinality, threadJoinMatches, R_chunkBase, metric_restitchTime, restitchTimer, bucketCache);
                    //__itt_pause();

                    timer->update();
                    metric_joinTime += (double) timer->getElapsedMicros() / 1000000.0;

                    ffbStart = ffbEnd + 1;    // Move on to the next workload for flipflop buffer
                    ++metric_sChunksProcessed;
                }

                // Increment the workload
                threadWorkBase += (uint32_t)(numThreads * threadWorkloadCardinality);
            }

            // If there is still any restitching needing to be done, do it now.
            if (outputIndex != 0) {
                restitchTuples((uint32_t)R_chunkBase, outputBuffer, outputIndex, metric_restitchTime, restitchTimer, wto->_options);
            }

            // Clean up
            delete outputBuffer;
            outputBuffer = nullptr;

            delete timer;
            timer = nullptr;
            delete restitchTimer;
            restitchTimer = nullptr;

            delete bucketCache;
            bucketCache = nullptr;

            // Update data
            wto->_threadSpinlock->lock();
            (*wto->_totalJoinMatches) += threadJoinMatches;
            (*wto->_totalFlipFlopTime) += metric_flipFlopTime;
            (*wto->_totalJoinTime) += metric_joinTime;
            (*wto->_totalSChunksProcessed) += metric_sChunksProcessed;
            (*wto->_totalRestitchTime) += metric_restitchTime;
            wto->_threadSpinlock->unlock();
            free(wto);
            return nullptr;
        }



        void * workerThread(SWorkerThreadOptions wto)
        {
            // Grab my local options
            uint32_t threadID = wto.threadID;
            uint32_t numThreads = wto.numThreads;
            uint64_t threadWorkloadCardinality = wto.threadWorkloadCardinality;
            uint32_t flipFlopCardinality = wto.flipFlopCardinality;
            uint64_t R_chunkBase = wto.R_chunkBase;

            // Other stuff
            uint32_t threadWorkBase(0);                // Current starting ordinal position of work in S.
            uint32_t threadWorkEnd(0);                // Ending ordinal position of the current work in S
            uint32_t threadActualWorkload(0);            // How much work we'd like to do per iteration
            uint32_t ffbStart(0);
            uint32_t ffbEnd(0);

            uint32_t outputIndex(0);                    // Where to write the next item of output.
            uint32_t outputBufferCardinality(0);        // Maximum number of items to write.
            uint32_t threadJoinMatches(0);

            // Thread metrics
            double metric_flipFlopTime(0);
            double metric_joinTime(0);
            double metric_restitchTime(0);
            uint32_t metric_sChunksProcessed(0);

            // Test out this cache
            SBucketCache *bucketCache = new SBucketCache();
            bucketCache->bucketID = -1;
            bucketCache->cache = new uint32_t[_bucketCacheSize];


            Timer *timer = new Timer();
            Timer *restitchTimer = new Timer();

            outputBufferCardinality = _options->getOutputBufferCardinality();

            //SBUN* outputBuffer = new SBUN[outputBufferCardinality];
            vector<tuple_t> *outputBuffer = new vector<tuple_t>(outputBufferCardinality);

            tuple_t *currentBuffer(nullptr);            // Pointer to the current post-flipflop buffer

            CFlipFlopBuffer2 *ffb = _FFBs[threadID];
            ffb->setRelation(_options->getRelationS());

            //CFlipFlopBuffer2* ffb2 = new CFlipFlopBuffer2(_options->getRelationS(), flipFlopCardinality, _options->getBitRadixLength(), _options->getMaxBitsPerFlipFlopPass());
            //CFlipFlopBuffer2* ffb2 = new CFlipFlopBuffer2(_options->getRelationS(), flipFlopCardinality, 1, _options->getMaxBitsPerFlipFlopPass());		// Disable flip-flop to measure effect

            // Calculate the starting point in S of the current workload
            threadWorkBase = threadID * (uint32_t)threadWorkloadCardinality;

            while (threadWorkBase < _options->getRelationS()->num_tuples)    // While there is still work to do...
            {
                threadActualWorkload = (uint32_t)threadWorkloadCardinality;    // The ideal workload
                if ((threadWorkBase + threadActualWorkload) >
                    _options->getRelationS()->num_tuples)    // Too much work to do?
                {
                    threadActualWorkload =
                            (uint32_t)(_options->getRelationS()->num_tuples - threadWorkBase);    // Lighten the workload
                }
                threadWorkEnd = threadWorkBase + threadActualWorkload - 1;

                // Now we do something similar for each flipFlop pass
                ffbStart = threadWorkBase;

                while (ffbStart <= threadWorkEnd) {
                    ffbEnd = ffbStart + flipFlopCardinality - 1;
                    if (ffbEnd > threadWorkEnd) {
                        ffbEnd = threadWorkEnd;
                    }

                    // Let's do it!
                    timer->update();
                    currentBuffer = ffb->doFlipFlop(ffbStart, ffbEnd, true);
                    timer->update();
                    metric_flipFlopTime += (double) timer->getElapsedMicros() / 1000000.0;

                    timer->update();

                    //__itt_resume();
                    doNLJProbing(currentBuffer, ffb->getCardinality(), outputBuffer, outputIndex,
                                 outputBufferCardinality, threadJoinMatches, (uint32_t)R_chunkBase, metric_restitchTime,
                                 restitchTimer);
                    //doNLJProbing2(currentBuffer, ffb->getCardinality(), outputBuffer, outputIndex, outputBufferCardinality, threadJoinMatches, R_chunkBase, metric_restitchTime, restitchTimer, bucketCache);
                    //__itt_pause();

                    timer->update();
                    metric_joinTime += (double) timer->getElapsedMicros() / 1000000.0;

                    ffbStart = ffbEnd + 1;    // Move on to the next workload for flipflop buffer
                    ++metric_sChunksProcessed;
                }

                // Increment the workload
                threadWorkBase += (uint32_t)(numThreads * threadWorkloadCardinality);
            }

            // If there is still any restitching needing to be done, do it now.
            if (outputIndex != 0) {
                restitchTuples((uint32_t)R_chunkBase, outputBuffer, outputIndex, metric_restitchTime, restitchTimer);
            }

            // Clean up
            delete outputBuffer;
            outputBuffer = nullptr;

            delete timer;
            timer = nullptr;
            delete restitchTimer;
            restitchTimer = nullptr;

            delete bucketCache;
            bucketCache = nullptr;

            // Update data
            _threadSpinlock.lock();
            _totalJoinMatches += threadJoinMatches;
            _totalFlipFlopTime += metric_flipFlopTime;
            _totalJoinTime += metric_joinTime;
            _totalSChunksProcessed += metric_sChunksProcessed;
            _totalRestitchTime += metric_restitchTime;
            _threadSpinlock.unlock();
            return nullptr;
        }

        static void *workerThreadHelper(void * param)
        {
//            CEnhancedSProcessor *processor = static_cast<CEnhancedSProcessor*>(param);
            auto *helperStruct = static_cast<WorkerThreadHelperStruct*>(param);
            helperStruct->_processor->workerThread(*helperStruct->_wto);
            free(helperStruct->_wto);
            free(helperStruct);
            return nullptr;
        }

        struct WorkerThreadHelperStruct
        {
            CEnhancedSProcessor *_processor;
            SWorkerThreadOptions *_wto;

            WorkerThreadHelperStruct(CEnhancedSProcessor *processor, SWorkerThreadOptions *wto):
                _processor(processor), _wto(wto)
            {}
        };

        CEnhancedRProcessorSTD *_vanillaR;
        SMetrics *_metrics;
        COptions *_options;
        CFlipFlopBuffer2 **_FFBs;
        uint32_t _bitMask;

        vector<pthread_t> _threads;
        CSpinlock _threadSpinlock;

        uint64_t _totalJoinMatches;

        double _totalFlipFlopTime;
        double _totalJoinTime;
        double _totalRestitchTime;
        uint32_t _totalSChunksProcessed;
        uint32_t _bucketCacheSize;
    };
}

#endif