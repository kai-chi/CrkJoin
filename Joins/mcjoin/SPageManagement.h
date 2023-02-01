/*
	This code is part of the MCJoin project.
	Authored by Steven Begley (sbegley@latrobe.edu.au) as part of my PhD candidature.
	La Trobe University,
	Melbourne, Australia.
*/
#ifndef MCJ_SPAGEMANAGEMENT_H
#define MCJ_SPAGEMANAGEMENT_H

namespace MCJ
{
    struct SPageManagement
    {
        uint32_t page;			// Number of a 64-bit chunk
        uint32_t offset;			// Bit offset from 64-bit chunk

        SPageManagement() : page(0), offset(0)
        {
        }

        __attribute__((always_inline)) void set(uint32_t _page, uint32_t _offset)
        {
            this->page = _page;
            this->offset = _offset;
        }


        // General-purpose offset calculator - the multiplication will make it a bit expensive.
        __attribute__((always_inline)) void calculateOffset(uint32_t itemCount, uint32_t bitLength)
        {
            // Thanks to Phil Ward for the idea of performing a modulus operation with a logical AND
            uint64_t totalBits = itemCount * bitLength;

            //page = uint32_t( totalBits / 64 );
            page = uint32_t( totalBits >> 6);

            //offset = totalBits % 64;
            offset = totalBits & 63;
        }
    };
}

#endif