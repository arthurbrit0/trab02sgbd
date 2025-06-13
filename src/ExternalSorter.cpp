#include "ExternalSorter.hpp"
#include "IoTracker.hpp"
#include <algorithm>
#include <cstdio>
#include <deque>
#include <queue>
#include <sstream>

namespace {
/* ---------- utilidades locais (split, readPage, writePage) --------------- */
void splitLine(const std::string& line, std::vector<std::string>& out)
{
    out.clear();
    std::string tok;
    std::stringstream ss(line);
    while (std::getline(ss, tok, CSV_SEP))
        out.emplace_back(std::move(tok));
}

bool readPage(std::ifstream& fin, Page& page, std::size_t colCnt,
              std::streampos* beginOut = nullptr)
{
    page.clear();
    if (beginOut) *beginOut = fin.tellg();

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

void writePage(std::ofstream& fout, const Page& page)
{
    for (const auto& t : page.tuples()) {
        for (std::size_t i = 0; i < t.cols.size(); ++i) {
            if (i) fout << CSV_SEP;
            fout << t.cols[i];
        }
        fout << '\n';
    }
    IoTracker::incWrite();
}

std::filesystem::path tmpName(const std::string& tag, int pass, int run)
{
    return std::filesystem::path{"tmp_" + tag + "_p" + std::to_string(pass) +
                                         "_r" + std::to_string(run) + ".csv"};
}
} // anonymous namespace

/* ------------------- PASSO 0 – geração dos runs -------------------------- */
static std::deque<std::filesystem::path>
pass0(const Table& tbl, std::size_t keyIdx, const std::string& tag)
{
    Table::PageCursor cur(tbl, tbl.header().size());

    std::deque<std::filesystem::path> runs;
    std::vector<Tuple> mem;
    mem.reserve(PAGS_BUFFER_MAX * TUPLAS_POR_PAG);   // 4 páginas

    Page pg;
    int runId = 0;
    while (cur.next(pg)) {
        for (auto& t : pg.tuples()) mem.emplace_back(std::move(t));
        if (mem.size() == mem.capacity()) {
            std::sort(mem.begin(), mem.end(),
                      [&](const Tuple& a, const Tuple& b)
                      { return a.cols[keyIdx] < b.cols[keyIdx]; });

            auto name = tmpName(tag, 0, runId++);
            std::ofstream fout(name);

            /* cabeçalho */
            const auto& hdr = tbl.header();
            for (std::size_t i = 0; i < hdr.size(); ++i) {
                if (i) fout << CSV_SEP;
                fout << hdr[i];
            }
            fout << '\n';
            IoTracker::incWrite();          // cabeçalho conta como 1 página

            Page out;
            for (auto& tup : mem) {
                if (out.full()) { writePage(fout, out); out.clear(); }
                out.emplace(std::move(tup));
            }
            if (!out.empty()) writePage(fout, out);

            runs.push_back(name);
            mem.clear();
        }
    }

    if (!mem.empty()) {
        std::sort(mem.begin(), mem.end(),
                  [&](const Tuple& a, const Tuple& b)
                  { return a.cols[keyIdx] < b.cols[keyIdx]; });

        auto name = tmpName(tag, 0, runId++);
        std::ofstream fout(name);

        const auto& hdr = tbl.header();
        for (std::size_t i = 0; i < hdr.size(); ++i) {
            if (i) fout << CSV_SEP;
            fout << hdr[i];
        }
        fout << '\n';
        IoTracker::incWrite();

        Page out;
        for (auto& tup : mem) {
            if (out.full()) { writePage(fout, out); out.clear(); }
            out.emplace(std::move(tup));
        }
        if (!out.empty()) writePage(fout, out);

        runs.push_back(name);
    }
    return runs;
}

/* -------------- merge de DOIS runs (usa 3 páginas na RAM) ----------------- */
static std::filesystem::path
mergeTwo(const std::filesystem::path& A,
         const std::filesystem::path& B,
         std::size_t keyIdx,
         const std::vector<std::string>& header,
         const std::string& tag,
         int passNo,
         int outId)
{
    std::ifstream fa(A), fb(B);
    std::string dummy;
    std::getline(fa, dummy);  IoTracker::incRead();    // cabeçalhos
    std::getline(fb, dummy);  IoTracker::incRead();

    Page pA, pB, out;
    std::streampos posA = 0, posB = 0;
    readPage(fa, pA, header.size(), &posA);
    readPage(fb, pB, header.size(), &posB);

    std::size_t ia = 0, ib = 0;

    auto outName = tmpName(tag, passNo, outId);
    std::ofstream fout(outName);

    /* escreve cabeçalho */
    for (std::size_t i = 0; i < header.size(); ++i) {
        if (i) fout << CSV_SEP;
        fout << header[i];
    }
    fout << '\n';
    IoTracker::incWrite();    // cabeçalho = 1 página

    while (!pA.empty() && !pB.empty()) {
        const auto& ta = pA.tuples()[ia];
        const auto& tb = pB.tuples()[ib];

        if (ta.cols[keyIdx] <= tb.cols[keyIdx]) {
            if (out.full()) { writePage(fout, out); out.clear(); }
            out.emplace(ta);               // cópia barata (pequenas strings)
            ++ia;
            if (ia == pA.tuples().size()) { readPage(fa, pA, header.size(), &posA); ia = 0; }
        } else {
            if (out.full()) { writePage(fout, out); out.clear(); }
            out.emplace(tb);
            ++ib;
            if (ib == pB.tuples().size()) { readPage(fb, pB, header.size(), &posB); ib = 0; }
        }
    }
    /* descarrega resto */
    auto flushRest = [&](Page& pg, std::size_t& idx, std::ifstream& fin,
                         std::streampos& pos){
        while (!pg.empty()) {
            if (idx == pg.tuples().size()) { readPage(fin, pg, header.size(), &pos); idx = 0; continue; }
            if (out.full()) { writePage(fout, out); out.clear(); }
            out.emplace(pg.tuples()[idx++]);
        }
    };
    flushRest(pA, ia, fa, posA);
    flushRest(pB, ib, fb, posB);

    if (!out.empty()) writePage(fout, out);
    return outName;
}

/* ----------------- passes sucessivos de merge ---------------------------- */
static std::deque<std::filesystem::path>
mergePass(std::deque<std::filesystem::path>& runs,
          std::size_t keyIdx,
          const std::vector<std::string>& header,
          const std::string& tag,
          int passNo)
{
    std::deque<std::filesystem::path> out;
    int id = 0;
    while (!runs.empty()) {
        auto A = runs.front(); runs.pop_front();
        if (runs.empty()) {               // run solitário – move para próxima fase
            out.push_back(A);
            break;
        }
        auto B = runs.front(); runs.pop_front();
        out.push_back(mergeTwo(A, B, keyIdx, header, tag, passNo, id++));
        std::remove(A.string().c_str());
        std::remove(B.string().c_str());
    }
    return out;
}

/* ------------------------- driver externo -------------------------------- */
std::filesystem::path externalSort(const Table& tbl,
                                   const std::string& colName,
                                   const std::string& tag)
{
    const std::size_t keyIdx = tbl.colIndex(colName);

    auto runs = pass0(tbl, keyIdx, tag);
    int passNo = 1;
    while (runs.size() > 1)
        runs = mergePass(runs, keyIdx, tbl.header(), tag, passNo++);
    return runs.front();           // arquivo final ordenado
}
