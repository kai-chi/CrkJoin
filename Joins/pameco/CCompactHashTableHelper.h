#ifndef PMC_CCOMPACTHASHTABLEHELPER_H
#define PMC_CCOMPACTHASHTABLEHELPER_H

namespace PMC
{
    class CCompactHashTableHelper
    {
    public:
        uint64_t getProjectedMemoryCost(const uint32_t maxItemCount, const uint32_t radixBits)
        {
            uint32_t dataBitsToStore(32 - radixBits);
            uint64_t result(0);

            result = memNeededInBytes(maxItemCount, dataBitsToStore);
            result += memNeededInBytes(maxItemCount, getBitsNeededForOffsetRepresentation(maxItemCount));

            return result;
        }

        uint32_t getBitsNeededForOffsetRepresentation(uint32_t itemCount)
        {
            double bits = log((double)itemCount) / log(2.0);

            return (uint32_t)ceil(bits);
        }

        uint64_t memNeededInBytes(const uint32_t count, const uint32_t bitsToStore)
        {
            uint64_t result(0);
            uint64_t totalBits = count;

            totalBits *= bitsToStore;

            result = totalBits >> 6;

            if (((totalBits)& 63) != 0)
            {
                ++result;	// Round up to next 64-bit boundary
            }

            result <<= 3;

            return result;
        }

    };
}

#endif //CCOMPACTHASHTABLEHELPER_H