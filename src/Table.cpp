#include "Table.hpp"
#include "IoTracker.hpp"
#include <sstream>
#include <stdexcept>

namespace {
/* --------------- utilitário: split de linha CSV -------------------------- */
void splitLine(const std::string& line, std::vector<std::string>& out)
{
    out.clear();
    std::string field;
    std::stringstream ss(line);
    while (std::getline(ss, field, CSV_SEP))
        out.emplace_back(std::move(field));
}

/* --------------- utilitário: lê 1 página do arquivo ---------------------- */
bool readPage(std::ifstream& fin, Page& page, std::size_t colCnt,
              std::streampos* pageBeginOut = nullptr)
{
    page.clear();
    if (pageBeginOut) *pageBeginOut = fin.tellg();

    std::string line;
    for (std::size_t i = 0; i < TUPLAS_POR_PAG && std::getline(fin, line); ) {
        if (line.empty()) continue;
        Tuple t;
        splitLine(line, t.cols);
        while (t.cols.size() < colCnt) t.cols.emplace_back("");
        page.emplace(std::move(t));
        ++i;
    }
    if (!page.empty()) { IoTracker::incRead(); return true; }
    return false;
}
} // anonymous namespace

/* ----------------------------- Table ------------------------------------- */
Table::Table(std::filesystem::path csvPath) : path_(std::move(csvPath))
{
    std::ifstream fin(path_);
    if (!fin) throw std::runtime_error("Não foi possível abrir " + path_.string());

    std::string headerLine;
    if (!std::getline(fin, headerLine))
        throw std::runtime_error("CSV vazio: " + path_.string());

    splitLine(headerLine, header_);
}

std::size_t Table::colIndex(const std::string& name) const
{
    for (std::size_t i = 0; i < header_.size(); ++i)
        if (header_[i] == name) return i;
    throw std::invalid_argument("Coluna " + name + " inexistente em " + path_.string());
}

/* ------------------------ PageCursor ------------------------------------- */
Table::PageCursor::PageCursor(const Table& tbl, std::size_t colCnt)
    : tbl_(tbl), colCnt_(colCnt), fin_(tbl.csvPath())
{
    std::string dummy;
    std::getline(fin_, dummy);          // cabeçalho
    IoTracker::incRead();
}

bool Table::PageCursor::next(Page& out)
{
    return readPage(fin_, out, colCnt_);
}

void Table::PageCursor::reset()
{
    fin_.clear();
    fin_.seekg(0);
    std::string dummy;
    std::getline(fin_, dummy);
    IoTracker::incRead();
}
