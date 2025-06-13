#pragma once
#include <cstddef>

/* ---------------------------------------------------------------------------
 *  Contador global de páginas lidas / gravadas.
 *  Cada vez que uma página de até 10 tuplas é transferida entre disco e RAM,
 *  incremente o contador correspondente (incRead / incWrite).
 * --------------------------------------------------------------------------*/
struct IoTracker {
    inline static std::size_t reads  = 0;   // páginas lidas
    inline static std::size_t writes = 0;   // páginas gravadas

    static void   reset()               { reads = writes = 0; }
    static void   incRead()             { ++reads;  }
    static void   incWrite()            { ++writes; }
    static size_t pagesWritten()        { return writes; }
    static size_t operations()          { return reads + writes; }
};
