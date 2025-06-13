#include "SortMergeJoin.hpp"
#include "IoTracker.hpp"
#include <fstream>
#include <sstream>

namespace {
/* ----------------- utilidades locais ------------------------------------ */
void writeHeader(std::ofstream& fout,
                 const std::vector<std::string>& hA,
                 const std::vector<std::string>& hB)
{
    for (std::size_t i = 0; i < hA.size(); ++i) {
        if (i) fout << CSV_SEP;
        fout << "A." << hA[i];
    }
    for (std::size_t i = 0; i < hB.size(); ++i) {
        fout << CSV_SEP << "B." << hB[i];
    }
    fout << '\n';
    IoTracker::incWrite();            // simples: cabeçalho = 1 página
}

/* readPage reaproveitado de outros módulos */
bool readPage(std::ifstream& fin, Page& page, std::size_t colCnt,
              std::streampos* beginOut = nullptr)
{
    page.clear();
    if (beginOut) *beginOut = fin.tellg();

    std::string line;
    for (std::size_t i = 0; i < TUPLAS_POR_PAG && std::getline(fin, line); ) {
        if (line.empty()) continue;
        Tuple t;
        std::stringstream ss(line); std::string tok;
        while (std::getline(ss, tok, CSV_SEP)) t.cols.emplace_back(std::move(tok));
        while (t.cols.size() < colCnt) t.cols.emplace_back("");
        page.emplace(std::move(t));
        ++i;
    }
    if (!page.empty()) { IoTracker::incRead(); return true; }
    return false;
}
} // anonymous namespace

/* ============================ Sort‑Merge‑Join ============================ */
JoinStats sortMergeJoin(const Table& A, const Table& B,
                        const std::string& colA, const std::string& colB,
                        const std::filesystem::path& outCsv)
{
    IoTracker::reset();

    /* 1. Ordena as duas relações ------------------------------------------ */
    const auto fAs = externalSort(A, colA, "A");
    const auto fBs = externalSort(B, colB, "B");

    /* 2. Abre fluxos ordenados -------------------------------------------- */
    std::ifstream fa(fAs), fb(fBs);
    if (!fa || !fb) throw std::runtime_error("Erro abrindo arquivos ordenados.");

    std::string dummy;
    std::getline(fa, dummy); IoTracker::incRead();   // pula cabeçalhos
    std::getline(fb, dummy); IoTracker::incRead();

    const auto& hA = A.header();
    const auto& hB = B.header();
    const std::size_t keyA = A.colIndex(colA);
    const std::size_t keyB = B.colIndex(colB);

    Page pA, pB, pBmark, out;
    std::size_t ia = 0, ib = 0, ibMark = 0;
    std::streampos posA = 0, posB = 0, posBmark = 0;

    readPage(fa, pA, hA.size(), &posA);
    readPage(fb, pB, hB.size(), &posB);

    std::ofstream fout(outCsv);
    writeHeader(fout, hA, hB);

    std::size_t tuplesOut = 0;

    while (!pA.empty() && !pB.empty()) {
        const auto& ta = pA.tuples()[ia];
        const auto& tb = pB.tuples()[ib];

        if (ta.cols[keyA] < tb.cols[keyB]) {
            ++ia;
            if (ia == pA.tuples().size()) { readPage(fa, pA, hA.size(), &posA); ia = 0; }
            continue;
        }
        if (ta.cols[keyA] > tb.cols[keyB]) {
            ++ib;
            if (ib == pB.tuples().size()) { readPage(fb, pB, hB.size(), &posB); ib = 0; }
            continue;
        }

        /* --------- match! marca posição em B ----------------------------- */
        const std::string currKey = ta.cols[keyA];
        pBmark = pB; ibMark = ib; posBmark = posB;

        /* ---- para cada tupla de A com a chave = currKey ----------------- */
        while (true) {
            /* reseta ponteiro de B para início do grupo duplicado ---------- */
            fb.clear(); fb.seekg(posBmark);
            pB = pBmark; ib = ibMark;

            while (true) {
                const auto& tb2 = pB.tuples()[ib];

                /* gera resultado (ta × tb2) --------------------------------*/
                Tuple res;
                res.cols.reserve(hA.size() + hB.size());
                res.cols.insert(res.cols.end(),
                                ta.cols.begin(), ta.cols.end());
                res.cols.insert(res.cols.end(),
                                tb2.cols.begin(), tb2.cols.end());

                if (out.full()) {            // flush se necessário
                    for (const auto& tp : out.tuples()) {
                        for (std::size_t i = 0; i < tp.cols.size(); ++i) {
                            if (i) fout << CSV_SEP;
                            fout << tp.cols[i];
                        }
                        fout << '\n';
                    }
                    IoTracker::incWrite();
                    out.clear();
                }
                out.emplace(std::move(res));
                ++tuplesOut;

                ++ib;
                if (ib == pB.tuples().size()) {
                    if (!readPage(fb, pB, hB.size(), &posB)) break;
                    ib = 0;
                }
                if (pB.tuples()[ib].cols[keyB] != currKey) break;
            }

            /* avança A para próxima tupla --------------------------------- */
            ++ia;
            if (ia == pA.tuples().size()) {
                if (!readPage(fa, pA, hA.size(), &posA)) { ia = 0; break; }
                ia = 0;
            }
            if (pA.tuples()[ia].cols[keyA] != currKey) break;
        }

        /* --------- após grupo, avança B ------------------------------- */
        fb.clear(); fb.seekg(posBmark);
        pB = pBmark; ib = ibMark;
        do {
            ++ib;
            if (ib == pB.tuples().size()) {
                if (!readPage(fb, pB, hB.size(), &posB)) { pB.clear(); break; }
                ib = 0;
            }
        } while (!pB.empty() && pB.tuples()[ib].cols[keyB] == currKey);
    }

    /* flush final da página de saída -------------------------------------- */
    if (!out.empty()) {
        for (const auto& tp : out.tuples()) {
            for (std::size_t i = 0; i < tp.cols.size(); ++i) {
                if (i) fout << CSV_SEP;
                fout << tp.cols[i];
            }
            fout << '\n';
        }
        IoTracker::incWrite();
    }

    JoinStats st;
    st.ioOps     = IoTracker::operations();
    st.pagesOut  = IoTracker::pagesWritten();
    st.tuplesOut = tuplesOut;
    return st;
}
