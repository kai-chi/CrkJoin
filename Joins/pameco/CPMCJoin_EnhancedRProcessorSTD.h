#ifndef PMC_CPMCJOIN_ENHANCEDRPROCESSORSTD_H
#define PMC_CPMCJOIN_ENHANCEDRPROCESSORSTD_H

#include "COptions.h"
#include "SMetrics.h"
#include "CFlipFlopBuffer2.h"
#include "CCompactHashTable1.5S.h"
#include <vector>
#include <pthread.h>
#include "CSpinLock.h"
#include "Timer.h"
#include "CBitRadixHistogram.h"

namespace PMC
{
    class CEnhancedRProcessorSTD
    {
    private:
        struct SWTHistogramParams
        {
            uint32_t threadID;
            uint32_t from;
            uint32_t to;
            uint32_t chunkCardinalityR;
        };

        struct SWTFlopFlop
        {
            uint32_t threadID;
            uint32_t from;
            uint32_t to;
            uint32_t offsetAdjustment;
        };

        struct SWTFlopFlopStatic
        {
            uint32_t threadID;
            uint32_t from;
            uint32_t to;
            uint32_t offsetAdjustment;
            tuple_t *_ffbOutput;
            CFlipFlopBuffer2 *_FFBs;
        };

    public:
        CEnhancedRProcessorSTD(COptions *options, SMetrics *metrics, CFlipFlopBuffer2 **FFBs, CSpinlock *spinlocks,
                               uint32_t totalSpinlocks, CSpinlock *partitionLocks) : _options(options),
                                                                                     _metrics(metrics),
                                                                                     _FFBs(FFBs), _cht(nullptr),
                                                                                     _ffbOutput(nullptr), _spinlocks(spinlocks),
                                                                                     _partitionLocks(partitionLocks),
                                                                                     _totalSpinlocks(totalSpinlocks),
                                                                                     _histogramEntriesPerSpinlock(0)
        {
            _numThreads = _options->getThreads();

//            uint32_t ffbSize(_options->getFlipFlopCardinality() / _numThreads);

            _cht = new CCompactHashTable(_options->getBitRadixLength(), _spinlocks, _totalSpinlocks, _partitionLocks);

            _ffbOutput = new tuple_t *[_numThreads];
        }

        ~CEnhancedRProcessorSTD()
        {
            if (_cht != nullptr) {
                delete _cht;
                _cht = nullptr;
            }

            if (_ffbOutput != nullptr) {
                delete _ffbOutput;
                _ffbOutput = nullptr;
            }
        }

        uint32_t scatterToCompactHashTable(const uint32_t from, const uint32_t to)
        {
            const uint32_t chunkCardinalityR(to - from);
            const uint32_t chunkBase(from);
            Timer *timer;
            timer = new Timer();

            uint32_t highestBucketCount(0);

            // First, we build our histograms & initialise the compact hash tables
            if (_options->getThreads() == 1) {
                // Only one choice here!
                logger(WARN, "[Hist: ST]... ");
                highestBucketCount = buildHistogramSingleThread(from, to, chunkCardinalityR, timer);
            } else if (_options->getMemoryConstraintMB() == 0) {
                logger(WARN, "[Hist: UC]... ");
                highestBucketCount = buildHistogramUnconstrainedMultiThread(from, to, chunkCardinalityR, timer);
            } else {
                if (_options->getThreads() >= 6) {
                    logger(WARN, "[Hist: MT]... ");
                    //buildHistogramMultiThreadedFFBs(from, to, chunkCardinalityR, timer);
                    highestBucketCount = buildHistogramMultiThreadedSimple(from, to, chunkCardinalityR, timer);
                } else {
                    logger(WARN, "[Hist: ST]... ");
                    highestBucketCount = buildHistogramSingleThread(from, to, chunkCardinalityR, timer);
                }
            }

            // Do flip flopping and write to compact hash table
            stageFlipFlopAndScatter(from, to, chunkBase, timer);

            // Maybe I can make this bit faster somehow?
            stageRollBackHistograms(timer);

            // ... and, done?
            delete timer;

            return highestBucketCount;
        }

        CCompactHashTable *getCompactHashTable()
        {
            return _cht;
        }

    private:
        void
        stageFlipFlopAndScatter(const uint32_t from, const uint32_t to, const uint32_t chunkBase, Timer *timer)
        {
            (void) chunkBase;
            uint32_t maximumFFBSize(_options->getFlipFlopCardinality() / _numThreads);
            uint32_t actualFFBSize(0);
            uint32_t ffbBase(from);
            uint32_t activeThreads(0);
            uint32_t offsetAdjustment(0);


            timer->update();

            for (uint32_t counter(0); counter < _numThreads; ++counter) {
                // Fiddle with FlipFlop buffer settings here
                _FFBs[counter]->setRelation(_options->getRelationR());
                _FFBs[counter]->setParams(_options->getBitRadixLength(), _options->getMaxBitsPerFlipFlopPass());
            }

//            SWTFlopFlop wtff[_numThreads];

            //kgo!
            while (ffbBase < to) {
                actualFFBSize = maximumFFBSize;
                if (actualFFBSize > (to - ffbBase)) {
                    actualFFBSize = (to - ffbBase);
                }

                // We need to calculate some value to add to the FFB offsets, based on how far this FFB starting point is from the chunk base. It should be the number
                // of tuples already flip flopped.
                offsetAdjustment = ffbBase - from;

                // Set up an FFB and pop it into the thread collection
//                SWTFlopFlop wtff {0,0,0,0};
//                wtff[activeThreads].from = ffbBase;
//                wtff[activeThreads].to = (ffbBase + actualFFBSize - 1);
//                wtff[activeThreads].threadID = activeThreads;
//                wtff[activeThreads].offsetAdjustment = offsetAdjustment;

                auto *swt = (SWTFlopFlopStatic*) malloc(sizeof(SWTFlopFlopStatic));
                swt->from = ffbBase;
                swt->to = (ffbBase + actualFFBSize - 1);
                swt->threadID = activeThreads;
                swt->offsetAdjustment = offsetAdjustment;
                swt->_ffbOutput = _ffbOutput[activeThreads];
                swt->_FFBs = _FFBs[activeThreads];

                pthread_t t;
//                WorkerThreadFlipFlopHelperStruct helper(this, wtff[activeThreads], activeThreads);
//                logger(DBG, "Will start workerThreadFlipFlopHelper-%d [%d, %d]", swt->threadID, swt->from, swt->to);
//                pthread_create(&t, nullptr, &CEnhancedRProcessorSTD::workerThreadFlipFlopHelper, (void *) &helper);
                pthread_create(&t, nullptr, &CEnhancedRProcessorSTD::workerThreadFlipFlop, (void *) swt);
                _threads.push_back(t);
//                _threads.push_back(thread(&CEnhancedRProcessorSTD::workerThreadFlipFlop, this, wtff));
                ++activeThreads;

                // Increment our base
                ffbBase += actualFFBSize;

                // Have we hit our thread limit? Join the threads and scatter them into PPM.
                if (activeThreads == _numThreads) {
                    // Join threads
                    joinThreads();
                    activeThreads = 0;

                    // Scatter
                    stageScatter();

                    // Reset flipflop buffer settings
                    resetFlipBlopBuffers();
                }
            }

            // Do we still have any threads active? Join them and scatter into PPM.
            if (activeThreads != 0) {
                joinThreads();
                activeThreads = 0;

                stageScatter();

                resetFlipBlopBuffers();
            }

            timer->update();
            _metrics->time_flipFlop_r += (double) timer->getElapsedMicros() / 1000000.0;

            // Some of the flipFlop time was actually spent scattering into the compact hash table. We'll need to address that later.
        }

        void stageScatter()
        {
            Timer time_scatterR;

            time_scatterR.update();

            //__itt_resume();
            //singleThreadScatter();
            //__itt_pause();

            // This shouldn't be enabled when doing stuff "for reals"
            ///*
            for (uint32_t threadID(0); threadID < _numThreads; ++threadID) {
                //_threads.push_back(thread(&CEnhancedRProcessorSTS::workerThreadScatter, this, threadID));
//                _threads.push_back(thread(&CEnhancedRProcessorSTD::workerThreadScatter2, this, threadID, _numThreads));
                pthread_t t;
                WorkerThreadScatter2HelperStruct helper(this, threadID, _numThreads);
                pthread_create(&t, nullptr, &CEnhancedRProcessorSTD::workerThreadScatter2Helper, (void *) &helper);
                _threads.push_back(t);
            }

            joinThreads();
            //*/

            time_scatterR.update();
            _metrics->time_scatter += (double) time_scatterR.getElapsedMicros() / 1000000.0;
        }

        void singleThreadScatter()
        {
            // Iterate over all FFBs, scatter tuples of interest

            tuple_t *p(nullptr);
            uint32_t ffbCardinality(0);

            for (uint32_t ffbCounter(0); ffbCounter < _numThreads; ++ffbCounter) {
                if (_ffbOutput[ffbCounter] != nullptr)    // Anything in this FFB?
                {
                    p = _ffbOutput[ffbCounter];
                    ffbCardinality = _FFBs[ffbCounter]->getCardinality();

                    for (uint32_t tupleIndex(0); tupleIndex < ffbCardinality; ++tupleIndex)    // Walk over all tuples
                    {
                        _cht->scatterKey(p);

                        ++p;
                    }
                }
            }
        }


        void workerThreadScatter(uint32_t threadID)
        {
            // Each thread scatters the contents of a FFB

            tuple_t *p(nullptr);
            uint32_t ffbCardinality(0);

            if (threadID % 2 == 1) {
                // *** Iterate forward ***

                if (_ffbOutput[threadID] != nullptr)    // Anything in this FFB?
                {
                    ffbCardinality = _FFBs[threadID]->getCardinality();
                    p = _ffbOutput[threadID];

                    for (uint32_t tupleIndex(0); tupleIndex < ffbCardinality; ++tupleIndex)    // Walk over all tuples
                    {
                        _cht->scatterKey(p);

                        ++p;
                    }
                }
            } else {
                // *** Iterate backward ***

                if (_ffbOutput[threadID] != nullptr)    // Anything in this FFB?
                {
                    ffbCardinality = _FFBs[threadID]->getCardinality();
                    p = _ffbOutput[threadID];
                    p += (ffbCardinality - 1);

                    for (uint32_t tupleIndex(0); tupleIndex < ffbCardinality; ++tupleIndex)    // Walk over all tuples
                    {
                        _cht->scatterKey(p);

                        --p;
                    }
                }
            }

        }

        void *workerThreadScatter2(uint32_t threadID, uint32_t totalThreads)
        {
            // Each thread scatters the contents of a FFB
//            logger(DBG, "Start workerThreadScatter2-%d/%d", threadID, totalThreads);
            tuple_t *p(nullptr);
            uint32_t ffbCardinality(0);

            if (_ffbOutput[threadID] != nullptr)    // Anything in this FFB?
            {
                ffbCardinality = _FFBs[threadID]->getCardinality();
                p = _ffbOutput[threadID];

                uint32_t startPosition((ffbCardinality / totalThreads) * threadID);
                p += startPosition;

                tuple_t *endP = _ffbOutput[threadID] + ffbCardinality;

                for (uint32_t tupleIndex(0); tupleIndex < ffbCardinality; ++tupleIndex)    // Walk over all tuples
                {
                    _cht->scatterKey(p);

                    ++p;

                    if (p == endP) {
                        p = _ffbOutput[threadID];
                    }
                }
            }
            return nullptr;
        }

        static void *workerThreadScatter2Helper(void *params)
        {
            auto *helper = static_cast<WorkerThreadScatter2HelperStruct *>(params);
            return helper->_processor->workerThreadScatter2(helper->_threadID, helper->_totalThreads);
        }

        struct WorkerThreadScatter2HelperStruct
        {
            CEnhancedRProcessorSTD *_processor;
            uint32_t _threadID;
            uint32_t _totalThreads;

            WorkerThreadScatter2HelperStruct(CEnhancedRProcessorSTD *processor, uint32_t threadID,
                                             uint32_t totalThreads) :
                    _processor(processor), _threadID(threadID), _totalThreads(totalThreads)
            {}
        };

        static void *workerThreadFlipFlop(void *param)
        {
            auto arg = static_cast<SWTFlopFlopStatic*>(param);

//            logger(DBG, "start workerThreadFlipFlop-%d [%d, %d], param=%p", arg->threadID, arg->from, arg->to,
//                   param);
            //_FFBs[params.threadID]->setRelation(_options->getRelationR());
            arg->_ffbOutput = arg->_FFBs->doFlipFlop(arg->from, arg->to, false,
                                                                             arg->offsetAdjustment);

            // Test dump
            /*
            tuple_t* p = _ffbOutput[params.threadID];
            for (uint32_t index(0); index < _ffb2[params.threadID]->getCardinality(); ++index)
            {
                cout << "K: " << p->key << ", V: " << p->payload << endl;
                ++p;
            }
            */
            free(arg);
            return nullptr;
        }

        void *workerThreadFlipFlop(SWTFlopFlop params, uint32_t threadID)
        {
            (void) threadID;
//            logger(DBG, "start workerThreadFlipFlop-%d (%d)", params.threadID, threadID);
            //_FFBs[params.threadID]->setRelation(_options->getRelationR());
            _ffbOutput[params.threadID] = _FFBs[params.threadID]->doFlipFlop(params.from, params.to, false,
                                                                             params.offsetAdjustment);

            // Test dump
            /*
            tuple_t* p = _ffbOutput[params.threadID];
            for (uint32_t index(0); index < _ffb2[params.threadID]->getCardinality(); ++index)
            {
                cout << "K: " << p->key << ", V: " << p->payload << endl;
                ++p;
            }
            */
            return nullptr;
        }

        static void *workerThreadFlipFlopHelper(void *param)
        {
            auto *helper = static_cast<WorkerThreadFlipFlopHelperStruct *>(param);
//            logger(DBG, "Prestart workerThreadFlipFlop-%d (%d)", helper->_swt.threadID, helper->_threadID);
            return helper->_processor->workerThreadFlipFlop(helper->_swt, helper->_threadID);
        }

        struct WorkerThreadFlipFlopHelperStruct
        {
            CEnhancedRProcessorSTD *_processor;
            SWTFlopFlop _swt;
            uint32_t _threadID;

            WorkerThreadFlipFlopHelperStruct(CEnhancedRProcessorSTD *processor, SWTFlopFlop swt, uint32_t threadID) :
                    _processor(processor), _swt(swt), _threadID(threadID)
            {}
        };

        void stageRollBackHistograms(Timer *timer)
        {
            timer->update();
            _cht->getHistogram()->rollbackPrefixSum();
            timer->update();
            _metrics->time_histogram_rollback_r += (double) timer->getElapsedMicros() / 1000000.0;
        }

        uint32_t
        buildHistogramSingleThread(const uint32_t from, const uint32_t to, uint32_t chunkSize, Timer *timer)
        {
            uint32_t highestBucketCount(0);

            timer->update();

            _cht->getHistogram()->reset();
            _cht->getHistogram()->buildHistogramKey(_options->getRelationR(), from, to);

            highestBucketCount = _cht->getHistogram()->buildPrefixSum();

            _cht->resetBuffers(_cht->getHistogram()->getItemCount(), chunkSize);

            timer->update();
            _metrics->time_histogram_build_r += (double) timer->getElapsedMicros() / 1000000.0;

            return highestBucketCount;
        }

        uint32_t
        buildHistogramUnconstrainedMultiThread(const uint32_t from, const uint32_t to, uint32_t chunkSize, Timer *timer)
        {
            timer->update();

            // Each thread gets its own localised histogram to use
            // Chop up input relation
            uint32_t totalRecords(to - from);
            uint32_t increments(totalRecords / _numThreads);
            uint32_t threadCount(0);

            uint32_t radixBitMask(_cht->getHistogram()->getRadixMask());
            uint32_t histogramSize(_cht->getHistogram()->getNumberOfPartitions());
            uint32_t **localHistograms = new uint32_t *[_numThreads - 1]();

            uint32_t highestBucketCount(0);

            _cht->getHistogram()->reset();

            // Give each thread a subset of the input relation to work with
            for (; threadCount < (_numThreads - 1); ++threadCount) {
                localHistograms[threadCount] = new uint32_t[histogramSize]();
                pthread_t t;
                BuildHistogramKeyLocalHistWorkerStruct helper(this, from + (threadCount * increments),
                                                              from + ((threadCount + 1) * increments),
                                                              localHistograms[threadCount], radixBitMask);
                pthread_create(&t, nullptr, &CEnhancedRProcessorSTD::buildHistogramKeyLocalHistWorkerHelper,
                               (void *) &helper);
                _threads.push_back(t);
//                _threads.push_back(thread(&CEnhancedRProcessorSTD::buildHistogramKeyLocalHistWorker, this,
//                                          from + (threadCount * increments), from + ((threadCount + 1) * increments),
//                                          localHistograms[threadCount], radixBitMask));
                //buildHistogramKeyLocalHistWorker(from + (threadCount * increments), from + ((threadCount + 1) * increments), localHistograms[threadCount], radixBitMask);
            }

            buildHistogramKeyLocalHistWorker(from + (threadCount * increments), to,
                                             _cht->getHistogram()->getHistogramArray(), radixBitMask);
            joinThreads();

            // Give each thread a range in the histograms to handle
            uint32_t histogramRangeSize(histogramSize / _numThreads);
            for (threadCount = 0; threadCount < (_numThreads - 1); ++threadCount) {
                pthread_t t;
                MergeLocalHistogramsWorkerStruct helper(this, (threadCount * histogramRangeSize),
                                                        ((threadCount + 1) * histogramRangeSize),
                                                        localHistograms);
                pthread_create(&t, nullptr, &CEnhancedRProcessorSTD::mergeLocalHistogramsWorkerHelper,
                               (void *) &helper);
                _threads.push_back(t);
//                _threads.push_back(thread(&CEnhancedRProcessorSTD::mergeLocalHistogramsWorker, this,
//                                          (threadCount * histogramRangeSize), ((threadCount + 1) * histogramRangeSize),
//                                          localHistograms));
                //mergeLocalHistogramsWorker((threadCount * histogramRangeSize), ((threadCount + 1) * histogramRangeSize), localHistograms);
            }

            mergeLocalHistogramsWorker((threadCount * histogramRangeSize), histogramSize, localHistograms);
            joinThreads();

            for (threadCount = 0; threadCount < (_numThreads - 1); ++threadCount) {
                delete[] localHistograms[threadCount];
            }
            delete[] localHistograms;

            // Back to single thread
            _cht->getHistogram()->addToItemCount(totalRecords);

            highestBucketCount = _cht->getHistogram()->buildPrefixSum();

            _cht->resetBuffers(_cht->getHistogram()->getItemCount(), chunkSize);

            timer->update();
            _metrics->time_histogram_build_r += (double) timer->getElapsedMicros() / 1000000.0;


            return highestBucketCount;
        }

        void *
        mergeLocalHistogramsWorker(uint32_t from, uint32_t to, uint32_t **localHistograms)
        {
//            logger(DBG, "Start mergeLocalHistogramsWorker [%d, %d]", from, to);
            uint32_t *h(_cht->getHistogram()->getHistogramArray());

            // Iterate through all localised histograms
            for (uint32_t histIndex(0); histIndex < (_numThreads - 1); ++histIndex) {
                // Iterate through the range governed by this thread
                for (uint32_t bucket(from); bucket < to; ++bucket) {
                    //h[bucket] += localHistograms[histIndex][bucket];
                    uint32_t a(h[bucket]);
                    uint32_t b(localHistograms[histIndex][bucket]);

                    a += b;
                    h[bucket] = a;
                }
            }
            return nullptr;
        }

        static void *mergeLocalHistogramsWorkerHelper(void *param)
        {
            MergeLocalHistogramsWorkerStruct *helper = static_cast<MergeLocalHistogramsWorkerStruct *>(param);
            return helper->_processor->mergeLocalHistogramsWorker(helper->_from, helper->_to, helper->_localHistograms);
        }

        struct MergeLocalHistogramsWorkerStruct
        {
            CEnhancedRProcessorSTD *_processor;
            uint32_t _from;
            uint32_t _to;
            uint32_t **_localHistograms;

            MergeLocalHistogramsWorkerStruct(CEnhancedRProcessorSTD *processor, uint32_t from, uint32_t to,
                                             uint32_t **localHistograms) :
                    _processor(processor), _from(from), _to(to), _localHistograms(localHistograms)
            {}
        };

        void *
        buildHistogramKeyLocalHistWorker(const uint32_t from, const uint32_t to, uint32_t *histogram,
                                         uint32_t radixBitMask)
        {
//            logger(DBG, "Start buildHistogramKeyLocalHistWorker[%d, %d]", from, to);
            uint32_t recordCount(to - from);
            tuple_t *p = &(_options->getRelationR()->tuples[from]);
            uint32_t maskedValue(0);

            for (uint32_t counter = 0; counter < recordCount; ++counter) {
                maskedValue = p->key & radixBitMask;
                ++(histogram[maskedValue]);

                ++p;
            }
            return nullptr;
        }

        static void *buildHistogramKeyLocalHistWorkerHelper(void *params)
        {
            BuildHistogramKeyLocalHistWorkerStruct *helper = static_cast<BuildHistogramKeyLocalHistWorkerStruct *>(params);
            return helper->_processor->buildHistogramKeyLocalHistWorker(helper->_from, helper->_to, helper->_histogram,
                                                                        helper->_radixBitMask);
        }

        struct BuildHistogramKeyLocalHistWorkerStruct
        {
            CEnhancedRProcessorSTD *_processor;
            uint32_t _from;
            uint32_t _to;
            uint32_t *_histogram;
            uint32_t _radixBitMask;

            BuildHistogramKeyLocalHistWorkerStruct(CEnhancedRProcessorSTD *processor, uint32_t from, uint32_t to,
                                                   uint32_t *histogram, uint32_t radixBitMask) :
                    _processor(processor), _from(from), _to(to), _histogram(histogram), _radixBitMask(radixBitMask)
            {}
        };

        uint32_t
        buildHistogramMultiThreadedSimple(const uint32_t from, const uint32_t to, uint32_t chunkSize, Timer *timer)
        {
            uint32_t totalRecords(to - from);
            uint32_t increments(totalRecords / _numThreads);
            uint32_t threadCount(0);
            uint32_t highestBucketCount(0);

            timer->update();

            _cht->getHistogram()->reset();

            // Chop up input relation

            if (_totalSpinlocks != 0) {
                _histogramEntriesPerSpinlock = (_cht->getHistogram()->getNumberOfPartitions() / _totalSpinlocks) + 1;
            }

            for (; threadCount < (_numThreads - 1); ++threadCount) {
                //buildHistogramKeySimpleWorker(from + (threadCount * increments), from + ((threadCount + 1) * increments));
                pthread_t t;
                BuildHistogramKeySimpleWorkerStruct helper(this, from + (threadCount * increments),
                                                           from + ((threadCount + 1) * increments));
                pthread_create(&t, nullptr, &CEnhancedRProcessorSTD::buildHistogramKeySimpleWorkerHelper,
                               (void *) &helper);
                _threads.push_back(t);
//                _threads.push_back(thread(&CEnhancedRProcessorSTD::buildHistogramKeySimpleWorker, this,
//                                          from + (threadCount * increments), from + ((threadCount + 1) * increments)));
            }

            buildHistogramKeySimpleWorker(from + (threadCount * increments), to);

            joinThreads();

            highestBucketCount = _cht->getHistogram()->buildPrefixSum();
            _cht->resetBuffers(_cht->getHistogram()->getItemCount(), chunkSize);

            timer->update();
            _metrics->time_histogram_build_r += (double) timer->getElapsedMicros() / 1000000.0;

            return highestBucketCount;
        }

        void *buildHistogramKeySimpleWorker(const uint32_t from, const uint32_t to)
        {
//            logger(DBG, "Start buildHistogramKeySimpleWorker [%d, %d]", from, to);
            uint32_t recordCount(to - from);
            uint32_t *histogram(_cht->getHistogram()->getHistogramArray());
            tuple_t *p = &(_options->getRelationR()->tuples[from]);
            uint32_t maskedValue(0);
            uint32_t radixMask(_cht->getHistogram()->getRadixMask());
            uint32_t spinlockID(0);

            for (uint32_t counter = 0; counter < recordCount; ++counter) {
                maskedValue = p->key & radixMask;

                spinlockID = maskedValue / _histogramEntriesPerSpinlock;

                _spinlocks[spinlockID].lock();
                ++(histogram[maskedValue]);
                _spinlocks[spinlockID].unlock();

                ++p;
            }

            _itemCountSpinlock.lock();
            _cht->getHistogram()->addToItemCount(recordCount);
            _itemCountSpinlock.unlock();
            return nullptr;
        }

        static void *buildHistogramKeySimpleWorkerHelper(void *param)
        {
            BuildHistogramKeySimpleWorkerStruct *helper = static_cast<BuildHistogramKeySimpleWorkerStruct *>(param);
            return helper->_processor->buildHistogramKeySimpleWorker(helper->_from, helper->_to);
        }

        struct BuildHistogramKeySimpleWorkerStruct
        {
            CEnhancedRProcessorSTD *_processor;
            uint32_t _from;
            uint32_t _to;

            BuildHistogramKeySimpleWorkerStruct(CEnhancedRProcessorSTD *processor, uint32_t from, uint32_t to) :
                    _processor(processor), _from(from), _to(to)
            {}
        };

        uint32_t
        buildHistogramMultiThreadedFFBs(const uint32_t from, const uint32_t to, uint32_t chunkSize, Timer *timer)
        {
            uint32_t actualFFBSize(0);
            uint32_t ffbBase(from);
            uint32_t activeThreads(0);
            uint32_t offsetAdjustment(0);
            uint32_t maximumFFBSize(_options->getFlipFlopCardinality() / _numThreads);
            SWTFlopFlop wtff;

            uint32_t highestBucketCount(0);

            // We are going to force single integers into the FlipFlop buffer, instead of usual tuple_ts. We measure the capacity of the FlipFlop buffer to be the number of tuple_ts it can contain. Each
            // tuple_t consists of two integers. Thus, for the purpose of FlipFlopping the histogram, we can fit twice as many integers into a FlipFlop buffer as we can fit tuple_ts.

            maximumFFBSize = maximumFFBSize << 1;    // Double the capacity!

            timer->update();

            _cht->getHistogram()->reset();

            // *** Multi-threaded in this part
            for (uint32_t counter(0); counter < _numThreads; ++counter) {
                // Fiddle with FlipFlop buffer settings here
                _FFBs[counter]->setRelation(_options->getRelationR());
                //_FFBs[counter]->setParams(1, 1); // WHAT DO?
            }

            if (_totalSpinlocks != 0) {
                _histogramEntriesPerSpinlock = (_cht->getHistogram()->getNumberOfPartitions() / _totalSpinlocks) + 1;
            }

            while (ffbBase < to) {
                actualFFBSize = maximumFFBSize;
                if (actualFFBSize > (to - ffbBase)) {
                    actualFFBSize = (to - ffbBase);
                }

                // We need to calculate some value to add to the FFB offsets, based on how far this FFB starting point is from the chunk base. It should be the number
                // of tuples already flip flopped.
                offsetAdjustment = ffbBase - from;

                // Set up an FFB and pop it into the thread collection
                wtff.from = ffbBase;
                wtff.to = (ffbBase + actualFFBSize - 1);
                wtff.threadID = activeThreads;
                wtff.offsetAdjustment = offsetAdjustment;

                pthread_t t;
                WorkerThreadFlipFlopForBuildingHistogramStruct helper(this, wtff);
                pthread_create(&t, nullptr, &CEnhancedRProcessorSTD::workerThreadFlipFlopForBuildingHistogramHelper, (void*)&helper);
                _threads.push_back(t);
//                _threads.push_back(
//                        thread(&CEnhancedRProcessorSTD::workerThreadFlipFlopForBuildingHistogram, this, wtff));
                //workerThreadFlipFlopForBuildingHistogram(wtff);

                ++activeThreads;

                // Increment our base
                ffbBase += actualFFBSize;

                // Have we hit our thread limit? Join the threads and scatter them into PPM.
                if (activeThreads == _numThreads) {
                    // Join threads
                    joinThreads();

                    activeThreads = 0;

                    // Reset flipflop buffer settings
                    resetFlipBlopBuffers();
                }
            }

            // Do we still have any threads active? Join them and scatter into PPM.
            if (activeThreads != 0) {
                joinThreads();
                activeThreads = 0;

                resetFlipBlopBuffers();
            }

            // *** Back to single thread again (these steps are very quick in comparison to everything else done here)
            highestBucketCount = _cht->getHistogram()->buildPrefixSum();

            _cht->resetBuffers(_cht->getHistogram()->getItemCount(), chunkSize);

            timer->update();
            _metrics->time_histogram_build_r += (double) timer->getElapsedMicros() / 1000000.0;

            return highestBucketCount;
        }


        void *workerThreadFlipFlopForBuildingHistogram(SWTFlopFlop params)
        {
//            logger(DBG, "Start workerThreadFlipFlopForBuildingHistogram-%d [%d, %d]", params.threadID, params.from, params.to);
            _ffbOutput[params.threadID] = _FFBs[params.threadID]->doFlipFlopForBuildingHistogram(params.from,
                                                                                                 params.to);
            writeFlipFlopContentsToHistogram(params.threadID);
            //_ffbOutput[params.threadID] = _FFBs[params.threadID]->doGroupByUINT32(_memoryBankBitLength);
            //_ffbOutput[params.threadID] = _FFBs[params.threadID]->buildLSBGroupByIndexUINT32(_memoryBankBitLength, false);	// Double handling the output is a bit silly, but I did it for debugging!
            return nullptr;
        }

        static void *workerThreadFlipFlopForBuildingHistogramHelper(void *param)
        {
            WorkerThreadFlipFlopForBuildingHistogramStruct *helper =
                    static_cast<WorkerThreadFlipFlopForBuildingHistogramStruct *>(param);
            return helper->_processor->workerThreadFlipFlopForBuildingHistogram(helper->_swt);
        }

        struct WorkerThreadFlipFlopForBuildingHistogramStruct
        {
            CEnhancedRProcessorSTD *_processor;
            SWTFlopFlop _swt;

            WorkerThreadFlipFlopForBuildingHistogramStruct(CEnhancedRProcessorSTD *processor, SWTFlopFlop swt) :
                    _processor(processor), _swt(swt)
            {}
        };

        void writeFlipFlopContentsToHistogram(uint32_t threadID)
        {
            uint32_t *p;
            uint32_t *h;

            uint32_t ffbCardinality(0);
            uint32_t spinlockID(0);

            uint32_t key(0);
            uint32_t maskedValue;
            uint32_t radixMask(_cht->getHistogram()->getRadixMask());

            if (_ffbOutput[threadID] != nullptr) {
                ffbCardinality = _FFBs[threadID]->getCardinality();
                if (ffbCardinality != 0) {
                    h = _cht->getHistogram()->getHistogramArray();
                    p = (uint32_t *) _ffbOutput[threadID];

                    uint32_t startPosition((ffbCardinality / _numThreads) * threadID);
                    p += startPosition;

                    uint32_t *endP = (uint32_t *) _ffbOutput[threadID] + ffbCardinality;

                    for (uint32_t index(0); index < ffbCardinality; ++index) {
                        key = *p;
                        maskedValue = key & radixMask;

                        if (_totalSpinlocks != 0) {
                            spinlockID = maskedValue / _histogramEntriesPerSpinlock;

                            _spinlocks[spinlockID].lock();
                            ++(h[maskedValue]);
                            _spinlocks[spinlockID].unlock();
                        } else {
                            ++(h[maskedValue]);
                        }

                        ++p;

                        if (p == endP) {
                            p = (uint32_t *) _ffbOutput[threadID];
                        }
                    }

                    _itemCountSpinlock.lock();
                    _cht->getHistogram()->addToItemCount(ffbCardinality);
                    _itemCountSpinlock.unlock();
                }
            }
        }

        void resetFlipBlopBuffers()
        {
            for (uint32_t counter(0); counter < _numThreads; ++counter) {
                _FFBs[counter]->reset();
                _ffbOutput[counter] = nullptr;
            }
        }

        uint32_t getBitsNeededForMemoryBanks(uint32_t numThreads)
        {
            double bits = log((double) numThreads) / log(2.0);

            return (uint32_t) ceil(bits);
        }

        void joinThreads()
        {
            for (auto &thread: _threads) {
                pthread_join(thread, nullptr);
            }
            _threads.clear();
        }

        bool isPowerOfTwo(uint32_t x)
        {
            return ((x != 0) && ((x & (~x + 1)) == x));
        }

        COptions *_options;
        SMetrics *_metrics;
        CFlipFlopBuffer2 **_FFBs;
        CCompactHashTable *_cht;
        tuple_t **_ffbOutput;

        CSpinlock _itemCountSpinlock;
        CSpinlock *_spinlocks;
        CSpinlock *_partitionLocks;
        uint32_t _totalSpinlocks;
        uint32_t _histogramEntriesPerSpinlock;

        uint32_t _numThreads;
        vector<pthread_t> _threads;
    };
}

#endif