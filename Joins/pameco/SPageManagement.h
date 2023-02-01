#ifndef PMC_SPAGEMANAGEMENT_H
#define PMC_SPAGEMANAGEMENT_H

namespace PMC
{
    struct SPageManagement
    {
        uint32_t page;            // Number of a 64-bit chunk
        uint32_t offset;            // Bit offset from 64-bit chunk

        SPageManagement() : page(0), offset(0)
        {
        }

        void set(uint32_t _page, uint32_t _offset)
        {
            this->page = _page;
            this->offset = _offset;
        }


        // General-purpose offset calculator - the multiplication will make it a bit expensive.
        void calculateOffset(uint64_t itemCount, uint32_t bitLength)
        {
            uint64_t totalBits(itemCount * bitLength);

            //page = uint32_t( totalBits / 64 );
            page = uint32_t(totalBits >> 6);

            //offset = totalBits % 64;
            offset = totalBits & 63;
        }

        void calculateOffset(uint64_t totalBits)
        {
            page = uint32_t(totalBits >> 6);
            offset = totalBits & 63;
        }
    };
}

#endif