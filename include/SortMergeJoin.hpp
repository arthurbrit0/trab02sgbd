#pragma once
#include "ExternalSorter.hpp"

struct JoinStats {
    std::size_t ioOps     = 0;   // leituras + gravações
    std::size_t pagesOut  = 0;   // páginas geradas no resultado
    std::size_t tuplesOut = 0;   // tuplas no resultado
};

/* ============================================================================
 *  Sort‑Merge‑Join clássico, usando no máximo 4 páginas simultâneas:
 *      – 1 págs de A, 1 págs de B, 1 págs de saída, 1 págs cópia‑marcada.
 *  O resultado é escrito em `outCsv`.
 * ===========================================================================*/
JoinStats sortMergeJoin(const Table&      A,
                        const Table&      B,
                        const std::string& colA,
                        const std::string& colB,
                        const std::filesystem::path& outCsv);
