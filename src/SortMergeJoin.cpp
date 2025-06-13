#include "SortMergeJoin.hpp"
#include "IoTracker.hpp"
#include <fstream>
#include <sstream>
#include <stdexcept>

namespace {
// Escreve o cabeçalho com prefixos A. e B.
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
    IoTracker::incWrite();  // conta como 1 página escrita
}

// Reutiliza o leitor de páginas, atualizando o contador de I/Os.
bool readPage(std::ifstream& fin, Page& page, std::size_t colCnt,
              std::streampos* beginOut = nullptr)
{
    page.clear();
    if (beginOut) *beginOut = fin.tellg();
    std::string line;
    for (std::size_t i = 0; i < TUPLAS_POR_PAG && std::getline(fin, line); ) {
        if (line.empty()) continue;
        Tuple t;
        std::stringstream ss(line);
        std::string tok;
        while (std::getline(ss, tok, CSV_SEP))
            t.cols.emplace_back(std::move(tok));
        while (t.cols.size() < colCnt)
            t.cols.emplace_back("");
        page.emplace(std::move(t));
        ++i;
    }
    if (!page.empty()) { IoTracker::incRead(); return true; }
    return false;
}
} // namespace

JoinStats sortMergeJoin(const Table& A, const Table& B,
                        const std::string& colA, const std::string& colB,
                        const std::filesystem::path& outCsv)
{
    IoTracker::reset();

    // 1. Ordena as duas relações externamente
    const auto fAs = externalSort(A, colA, "A");
    const auto fBs = externalSort(B, colB, "B");

    // 2. Abre arquivos ordenados
    std::ifstream fa(fAs), fb(fBs);
    if (!fa || !fb)
        throw std::runtime_error("Erro abrindo arquivos ordenados.");

    // Pula cabeçalhos
    std::string dummy;
    std::getline(fa, dummy); IoTracker::incRead();
    std::getline(fb, dummy); IoTracker::incRead();

    const auto& hA  = A.header();
    const auto& hB  = B.header();
    const auto  keyA = A.colIndex(colA);
    const auto  keyB = B.colIndex(colB);

    Page pA, pB, pBmark, out;
    std::size_t ia = 0, ib = 0, ibMark = 0;
    std::streampos posA = 0, posB = 0, posBmark = 0;
    readPage(fa, pA, hA.size(), &posA);
    readPage(fb, pB, hB.size(), &posB);

    std::ofstream fout(outCsv);
    writeHeader(fout, hA, hB);

    std::size_t tuplesOut = 0;

    // Laço principal de junção
    while (!pA.empty() && !pB.empty()) {
        // Avança A até chave >= B
        while (!pA.empty() && !pB.empty() && pA.tuples()[ia].cols[keyA] < pB.tuples()[ib].cols[keyB]) {
            ++ia;
            if (ia == pA.tuples().size()) {
                if (!readPage(fa, pA, hA.size(), &posA)) { pA.clear(); break; }
                ia = 0;
            }
        }
        // Avança B até chave >= A
        while (!pA.empty() && !pB.empty() && pA.tuples()[ia].cols[keyA] > pB.tuples()[ib].cols[keyB]) {
            ++ib;
            if (ib == pB.tuples().size()) {
                if (!readPage(fb, pB, hB.size(), &posB)) { pB.clear(); break; }
                ib = 0;
            }
        }
        if (pA.empty() || pB.empty()) break;

        // Encontrou grupo de mesma chave
        const std::string currKey = pA.tuples()[ia].cols[keyA];
        // Marca posição de B para voltar ao início do grupo
        pBmark = pB; ibMark = ib; posBmark = posB;

        // Para cada tupla A com chave = currKey
        while (!pA.empty() && pA.tuples()[ia].cols[keyA] == currKey) {
            const auto& taCurr = pA.tuples()[ia];

            // Percorre grupo de B
            fb.clear(); fb.seekg(posBmark);
            pB = pBmark; ib = ibMark;
            while (!pB.empty() && pB.tuples()[ib].cols[keyB] == currKey) {
                const auto& tb2 = pB.tuples()[ib];

                if (out.full()) {
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

                Tuple res;
                res.cols.reserve(hA.size() + hB.size());
                res.cols.insert(res.cols.end(), taCurr.cols.begin(), taCurr.cols.end());
                res.cols.insert(res.cols.end(), tb2.cols.begin(),  tb2.cols.end());
                out.emplace(std::move(res));
                ++tuplesOut;

                ++ib;
                if (ib == pB.tuples().size()) {
                    if (!readPage(fb, pB, hB.size(), &posB)) break;
                    ib = 0;
                }
            }

            // Avança A
            ++ia;
            if (ia == pA.tuples().size()) {
                if (!readPage(fa, pA, hA.size(), &posA)) break;
                ia = 0;
            }
        }

        // Avança B além do grupo para evitar reprocessar
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

    // Flush final de saída
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
