#pragma once
#include "Table.hpp"

/* =========================================================================
 *  External Merge Sort
 *  – Gera runs de até 4 páginas (passo‑0)
 *  – Executa passes de merge 2‑way (cada pass usa 2 págs entrada + 1 saída)
 *  – Mantém, portanto, <= 4 páginas na RAM.
 *  – Devolve o caminho do CSV totalmente ordenado.
 * =========================================================================*/
std::filesystem::path externalSort(const Table&      tbl,
                                   const std::string& colName,
                                   const std::string& tag);
