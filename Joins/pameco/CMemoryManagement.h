#ifndef PMC_CMEMORYMANAGEMENT_H
#define PMC_CMEMORYMANAGEMENT_H

#include "COptions.h"
#include "CCompactHashTable1.5S.h"
#include "CSpinLock.h"
#include <limits>
#ifdef NATIVE_COMPILATION
#include "native_ocalls.h"
#include "Logger.h"
#else
#include <libcxx/cmath>
#include "Enclave.h"
#include "Enclave_t.h"
#endif

namespace PMC
{
    class CMemoryManagement
    {
    public:
        CMemoryManagement() : _r_bias(0)
        {

        }

        bool allocateMemoryForRBias(double bias, COptions *options)
        {
            // The bias value is the fraction of memory being allocated to the Compact Hash Table (the remainder going to the Flip-Flop buffer)

            bool result(true);

            uint64_t total_memoryAllocated(0);
            uint32_t radixBits(0);
            uint32_t histogramSize(0);

            uint64_t R_bytes(0);
            uint64_t S_bytes(0);

            uint32_t R_idealTuples(0);
            uint32_t S_idealTuples(0);

            uint64_t memoryConstraint(0);

            uint32_t spinlockSize(0);

            memoryConstraint = options->getMemoryConstraintMB();
            total_memoryAllocated = memoryConstraint * 1024 * 1024;	// Measured in bytes

            radixBits = options->getBitRadixLength();

            // Remove histogram cost from memory allocation
            histogramSize = 4 * uint32_t(std::pow(2.0, (double)radixBits));

            if (histogramSize >= total_memoryAllocated)
            {
                return false;	// Not enough memory to perform the join with the supplied parameters
            }

            total_memoryAllocated -= histogramSize;

            // Calculate total spinlock size
            spinlockSize = sizeof(CSpinlock) * options->getSpinlocks();
            total_memoryAllocated -= spinlockSize;


            R_bytes = uint64_t((double)total_memoryAllocated * bias);
            R_idealTuples = numberOfPackedRecords((R_bytes), 32 - radixBits);

            CCompactHashTable* ppm = new CCompactHashTable(radixBits, nullptr, 0, nullptr);
            S_bytes = total_memoryAllocated - ppm->getProjectedMemoryCost(R_idealTuples);
            S_idealTuples = uint32_t(S_bytes / 16);

            _r_idealTuples = R_idealTuples;
            _s_idealTuples = S_idealTuples;
            _r_bias = (float)ppm->getProjectedMemoryCost(R_idealTuples) / (float)total_memoryAllocated;

            delete ppm;

            return result;
        }

        bool optimiseForMemoryConstraint(COptions *options)
        {
            bool result(true);

            CCompactHashTableHelper chth;

            uint64_t total_memoryAllocated(0);
            uint64_t memoryConstraintMB(0);
            uint32_t histogramSize(0);
            uint32_t R_cardinality(0);

            uint64_t R_maxTuples(0);

            uint32_t R_idealTuples(0);
            uint32_t S_idealTuples(0);

            uint64_t R_chunkMemoryAllocated(0);
            uint64_t S_chunkMemoryAllocated(0);

            uint32_t spinlockSize(0);
            uint32_t partitionLocksSize(0);

            double R_chunkCount;

            R_cardinality = (uint32_t)options->getRelationR()->num_tuples;
            memoryConstraintMB = options->getMemoryConstraintMB();
            total_memoryAllocated = memoryConstraintMB * 1024 * 1024;	// Measured in bytes


            if (options->getTechLevel() == 2)
            {
                options->setBitRadixLength(24);
                histogramSize = 4 * uint32_t(std::pow(2.0, (double)options->getBitRadixLength()));

                while (histogramSize > (total_memoryAllocated / 4))
                {
                    options->setBitRadixLength(options->getBitRadixLength() - 1);	// Shrink the histogram!
                    histogramSize = 4 * uint32_t(std::pow(2.0, (double)options->getBitRadixLength()));

                    if (options->getBitRadixLength() == 1)
                    {
                        logger(ERROR, "Histogram size allocator broke quite horribly. Well done.");
                        return false;
                    }
                }
            }
            else
            {
                histogramSize = 4 * uint32_t(std::pow(2.0, (double)options->getBitRadixLength()));
            }

            if (histogramSize >= total_memoryAllocated)
            {
                logger(ERROR, "Histogram is too large for the given memory constraint!");
                return false;	// Not enough memory to perform the join with the supplied parameters
            }

            // Remove histogram cost from memory allocation
            total_memoryAllocated -= histogramSize;



            // Work out a conservative estimation of the max number of compressed tuples we can force into an R chunk with the available memory
            R_maxTuples = numberOfPackedRecords((total_memoryAllocated), 32 - options->getBitRadixLength());

            // How many chunks do we need to process all of R? (whole chunks only!)
            R_chunkCount = ceil((float)R_cardinality / (float)R_maxTuples);


            // Now we work out the minimum allocation needed to create this chunk size. How many tuples are in the chunk?
            R_idealTuples = (uint32_t)ceil(R_cardinality / R_chunkCount);

            // Calculate total spinlock size and apply cost
            if (options->getThreads() > 1)
            {
                uint32_t spinlockCount = R_idealTuples / options->getSpinlockDivisor();
                options->setSpinlocks(spinlockCount);
                partitionLocksSize = ((1 << options->getBitRadixLength()) / 1024) * sizeof(CSpinlock);
            }

            if (partitionLocksSize >= total_memoryAllocated)
            {
                logger(ERROR, "Cannot allocate enough memory for partition spinlocks!");
                return false;
            }
            total_memoryAllocated -= partitionLocksSize;

            spinlockSize = sizeof(CSpinlock) * options->getSpinlocks();
            if (spinlockSize >= total_memoryAllocated)
            {
                logger(ERROR, "Cannot allocate enough memory for memory-range spinlocks!");
                return false;
            }
            total_memoryAllocated -= spinlockSize;


            R_chunkMemoryAllocated = chth.getProjectedMemoryCost(R_idealTuples, options->getBitRadixLength());

            S_chunkMemoryAllocated = total_memoryAllocated - R_chunkMemoryAllocated;
            S_idealTuples = (uint32_t)(S_chunkMemoryAllocated / 16);


            // Fine tune the number of R chunks
            while (((double)S_idealTuples / (double)R_idealTuples) < (1.0f / 32.0f))
            {
                ++R_chunkCount;
                R_idealTuples = (uint32_t)ceil(R_cardinality / R_chunkCount);

                R_chunkMemoryAllocated = chth.getProjectedMemoryCost(R_idealTuples, options->getBitRadixLength());

                S_chunkMemoryAllocated = total_memoryAllocated - R_chunkMemoryAllocated;
                S_idealTuples = (uint32_t)(S_chunkMemoryAllocated / 16);
            }


            _r_idealTuples = R_idealTuples;
            _s_idealTuples = S_idealTuples;

            if (_s_idealTuples > _r_idealTuples)
            {
                logger(WARN, "... weird lopsided stuff... ");
            }

            if (_s_idealTuples > options->getRelationS()->num_tuples)
            {
                logger(WARN, "... over-allocated FFB for S ... ");
            }

            _r_bias = (float)R_chunkMemoryAllocated / (float)total_memoryAllocated;

            return result;
        }

        double getRBias()
        {
            return _r_bias;
        }

        uint32_t getRIdealTuples()
        {
            return _r_idealTuples;
        }

        uint32_t getSIdealTuples()
        {
            return _s_idealTuples;
        }


    private:
        double _r_bias;
        uint32_t _r_idealTuples;
        uint32_t _s_idealTuples;

        bool AreEqual(double f1, double f2) {
            return (std::fabs(f1 - f2) <= std::numeric_limits<double>::epsilon() * std::fmax(std::fabs(f1), std::fabs(f2)));
        }

        double LambertW(const double z)
        {
            // LambertW by Keith Briggs
            // http://keithbriggs.info/software/LambertW.c

            int i;
            const double eps=4.0e-16, em1=0.3678794411714423215955237701614608;
            double p,e,t,w;


//            if (0.0==z) return 0.0;
            if (AreEqual(0.0, z)) return 0.0;

            if (z<-em1+1e-4) { // series near -em1 in sqrt(q)
                double q=z+em1,r=sqrt(q),q2=q*q,q3=q2*q;
                return
                        -1.0
                        +2.331643981597124203363536062168*r
                        -1.812187885639363490240191647568*q
                        +1.936631114492359755363277457668*r*q
                        -2.353551201881614516821543561516*q2
                        +3.066858901050631912893148922704*r*q2
                        -4.175335600258177138854984177460*q3
                        +5.858023729874774148815053846119*r*q3
                        -8.401032217523977370984161688514*q3*q;  // error approx 1e-16
            }
            /* initial approx for iteration... */

            if (z<1.0)
            { /* series near 0 */
                p=sqrt(2.0*(2.7182818284590452353602874713526625*z+1.0));
                w=-1.0+p*(1.0+p*(-0.333333333333333333333+p*0.152777777777777777777777));
            }
            else
            {
                w=log(z); /* asymptotic */
            }
            if (z>3.0) w-=log(w); /* useful? */

            for (i=0; i<10; i++) { /* Halley iteration */
                e=exp(w);
                t=w*e-z;
                p=w+1.0;
                t/=e*p-0.5*(p+1.0)*t/p;
                w-=t;
                if (fabs(t)<eps*(1.0+fabs(w))) return w; /* rel-abs error */
            }

            ocall_exit(1);
            return 0.0;
        }

        uint32_t numberOfPackedRecords(uint64_t memoryLimit, uint32_t bitsToStore)
        {
            double result = 0.5 * exp(LambertW(16.0 * log(2.0) * ((double)memoryLimit - 16.0) * exp(bitsToStore * log(2.0))) - (bitsToStore * log(2.0)));

            return (uint32_t)result;
        }

    };
}

#endif
