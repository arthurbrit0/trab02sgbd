#pragma once
#include "Page.hpp"
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

/* ========================================================================== *
 *  Representa um CSV no disco.  NÃO carrega tuplas em memória; serve
 *  apenas para disponibilizar o cabeçalho e criar cursores de páginas.
 * ========================================================================== */
class Table {
public:
    explicit Table(std::filesystem::path csvPath);

    std::size_t colIndex(const std::string& name) const;
    const std::vector<std::string>& header()  const { return header_; }
    const std::filesystem::path&    csvPath() const { return path_;  }

    /* --- Cursor sequencial de páginas --------------------------------------*/
    class PageCursor {
    public:
        PageCursor(const Table& tbl, std::size_t colCnt);
        bool next(Page& out);      // lê próxima página; devolve false em EOF
        void reset();              // reinicia ponteiro para início dos dados
    private:
        const Table&        tbl_;
        std::ifstream       fin_;
        std::size_t         colCnt_;
    };

private:
    std::filesystem::path  path_;
    std::vector<std::string> header_;
};
