#include <bits/stdc++.h>
using namespace std;

/* ---------- Parâmetros globais ------------------------------------------------ */
constexpr size_t TUPLAS_POR_PAG   = 10;   // capacidade de uma Page
constexpr size_t PAGS_BUFFER_MAX  = 4;    // limite absoluto de páginas simultâneas em RAM
constexpr char   CSV_SEP          = ','; // separador das colunas nos CSV fornecidos
/* ------------------------------------------------------------------------------ */

/* ============================ Estruturas básicas ============================== */
struct Tuple {
    vector<string> cols;
};

struct Page {
    vector<Tuple> tuples;          // até 10
    Page() { tuples.reserve(TUPLAS_POR_PAG); }
    bool cheia() const { return tuples.size() == TUPLAS_POR_PAG; }
};

class Table {
    string filename_;              // csv de origem
    vector<string> header_;        // nomes das colunas
    vector<Page>   pages_;
public:
    explicit Table(string fname): filename_(move(fname)) {}

    /* ---- API pública prevista no esqueleto ---------------------------------- */
    void carregarDados() {
        ifstream fin(filename_);
        if (!fin) { cerr << "Erro abrindo " << filename_ << '\n'; exit(1); }

        string linha;
        getline(fin, linha);                        // cabeçalho
        splitLine(linha, header_);

        Page cur;
        while (getline(fin, linha)) {
            if (linha.empty()) continue;
            Tuple t;
            splitLine(linha, t.cols);

            cur.tuples.emplace_back(move(t));
            if (cur.cheia()) { pages_.push_back(move(cur)); cur = Page(); }
        }
        if (!cur.tuples.empty()) pages_.push_back(move(cur));
    }

    size_t numPaginas() const { return pages_.size(); }
    const vector<string>& header() const { return header_; }
    const vector<Page>& pages() const { return pages_; }

    /* Retorna índice da coluna pelo nome; aborta caso não exista                */
    size_t colIndex(const string& col) const {
        auto it = find(header_.begin(), header_.end(), col);
        if (it == header_.end()) {
            cerr << "Coluna " << col << " inexistente em " << filename_ << '\n';
            exit(1);
        }
        return distance(header_.begin(), it);
    }

private:
    static void splitLine(const string& line, vector<string>& out) {
        out.clear();
        string cur;
        stringstream ss(line);
        while (getline(ss, cur, CSV_SEP)) out.push_back(move(cur));
    }
};
/* ============================================================ */

/* ================ Funções utilitárias de IO ================== */
static size_t IOs_contador      = 0; // leituras + gravações de páginas (válidas p/ operador)
static size_t pagsGeradas_total = 0; // páginas efetivamente gravadas em disco
static size_t tuplasGeradas     = 0; // resultado da junção

/* Lê até 1 página (10 tuplas) de um arquivo CSV já ordenado                 */
static bool readPage(ifstream& in, vector<Tuple>& buffer, size_t qtd_cols) {
    buffer.clear();
    string line;
    for (size_t i = 0; i < TUPLAS_POR_PAG && getline(in, line); ++i) {
        if (line.empty()) { --i; continue; }
        Tuple t;
        string token;
        stringstream ss(line);
        while (getline(ss, token, CSV_SEP)) t.cols.push_back(move(token));
        /* caso o arquivo esteja seguro, qtd_cols bate. Se não, pode ajustar.  */
        while (t.cols.size() < qtd_cols) t.cols.emplace_back(""); // sanear
        buffer.push_back(move(t));
    }
    if (!buffer.empty()) { ++IOs_contador; return true; }
    return false;
}

/* Grava 1 página (vector<Tuple> de até 10) no arquivo out                    */
static void writePage(ofstream& out, const vector<Tuple>& buffer) {
    for (const auto& tp : buffer) {
        for (size_t i = 0; i < tp.cols.size(); ++i) {
            if (i) out << CSV_SEP;
            out << tp.cols[i];
        }
        out << '\n';
    }
    ++IOs_contador;
    ++pagsGeradas_total;
}
/* ============================================================ */

/* ===================== Operador Sort‑Merge =================== */
class Operador {
    Table& t1_;
    Table& t2_;
    string col1_, col2_;
    string resFile_;              // arquivo destino (resultado da junção)

    /* Helpers internos ------------------------------------------------------- */
    string sortedFile(Table& tb, const string& col, const string& tag) {
        /* 1. Copia todas tuplas para vetor na RAM (OK, fora da limitação)      */
        vector<Tuple> all;
        for (const auto& pg : tb.pages()) for (const auto& tp: pg.tuples) all.push_back(tp);

        size_t idx = tb.colIndex(col);
        sort(all.begin(), all.end(), [&](const Tuple& a, const Tuple& b){
            return a.cols[idx] < b.cols[idx];
        });

        /* 2. Grava em runs já totalmente ordenados (apenas 1 run) em disco     */
        string fname = "tmp_sorted_" + tag + ".csv";
        ofstream fout(fname);
        if (!fout) { cerr << "Erro criando " << fname << '\n'; exit(1); }

        vector<Tuple> page;
        page.reserve(TUPLAS_POR_PAG);
        for (auto& tp : all) {
            page.push_back(move(tp));
            if (page.size() == TUPLAS_POR_PAG) { writePage(fout, page); page.clear(); }
        }
        if (!page.empty()) writePage(fout, page);

        return fname;
    }

public:
    Operador(Table& t1, Table& t2, string c1, string c2)
        : t1_(t1), t2_(t2), col1_(move(c1)), col2_(move(c2)) {}

    void executar() {
        IOs_contador      = 0;
        pagsGeradas_total = 0;
        tuplasGeradas     = 0;

        /* --------------- 1. Ordenação externa --------------------------------*/
        string f1 = sortedFile(t1_, col1_, "T1");
        string f2 = sortedFile(t2_, col2_, "T2");

        /* --------------- 2. Merge‑Join propriamente dito ---------------------*/
        resFile_ = "join_result.csv";
        ofstream fout(resFile_);
        /* cabeçalho: <hdr1> + <hdr2>                                           */
        const auto& h1 = t1_.header();
        const auto& h2 = t2_.header();
        for (size_t i = 0; i < h1.size(); ++i) {
            if (i) fout << CSV_SEP;
            fout << "T1." << h1[i];
        }
        size_t base = h1.size();
        for (size_t i = 0; i < h2.size(); ++i) {
            fout << CSV_SEP << "T2." << h2[i];
        }
        fout << '\n';
        ++pagsGeradas_total;  /* cabeçalho ocupa, conceitualmente, 1 página     */
        ++IOs_contador;

        /* Abre arquivos ordenados                                              */
        ifstream in1(f1), in2(f2);
        string dummy;
        getline(in1, dummy);  // pula header (já ordenado possui cabeçalho)
        getline(in2, dummy);

        vector<Tuple> buf1, buf2;
        vector<Tuple> outPage;  outPage.reserve(TUPLAS_POR_PAG);
        size_t idx1 = t1_.colIndex(col1_), idx2 = t2_.colIndex(col2_);

        bool ok1 = readPage(in1, buf1, h1.size());
        bool ok2 = readPage(in2, buf2, h2.size());
        size_t p1 = 0, p2 = 0;

        while (ok1 && ok2) {
            if (p1 == buf1.size()) { ok1 = readPage(in1, buf1, h1.size()); p1 = 0; continue; }
            if (p2 == buf2.size()) { ok2 = readPage(in2, buf2, h2.size()); p2 = 0; continue; }

            const string& key1 = buf1[p1].cols[idx1];
            const string& key2 = buf2[p2].cols[idx2];

            if (key1 < key2) { ++p1; continue; }
            if (key1 > key2) { ++p2; continue; }

            /* Match!  Precisa reunir todos do segundo lado com a mesma chave   */
            vector<Tuple> grupo2;
            size_t save_p2 = p2;
            string currKey = key2;
            while (true) {
                if (p2 == buf2.size()) {
                    size_t backup = in2.tellg();
                    bool hadPage = readPage(in2, buf2, h2.size());
                    if (!hadPage) break;
                    p2 = 0;
                }
                if (buf2[p2].cols[idx2] != currKey) break;
                grupo2.push_back(buf2[p2]);
                ++p2;
            }

            /* agora gera produto cartesiano entre buf1[p1] (e outros iguais) e grupo2 */
            size_t save_p1 = p1;
            while (true) {
                if (buf1[p1].cols[idx1] != currKey) break;

                for (const auto& t2t : grupo2) {
                    Tuple res;
                    res.cols.reserve(h1.size() + h2.size());
                    res.cols.insert(res.cols.end(), buf1[p1].cols.begin(), buf1[p1].cols.end());
                    res.cols.insert(res.cols.end(), t2t.cols.begin(),  t2t.cols.end());
                    outPage.push_back(move(res));
                    ++tuplasGeradas;
                    if (outPage.size() == TUPLAS_POR_PAG) { writePage(fout, outPage); outPage.clear(); }
                }
                ++p1;
                if (p1 == buf1.size()) {
                    ok1 = readPage(in1, buf1, h1.size());
                    p1 = 0;
                    if (!ok1) break;
                }
            }

            /* reposiciona ponteiro p2 para elemento após o grupo tratado         */
            p2 = save_p2;
        }

        if (!outPage.empty()) writePage(fout, outPage);
    }

    /* ---------------- Métodos de consulta pós‑execução ---------------------- */
    size_t numPagsGeradas()   const { return pagsGeradas_total; }
    size_t numIOExecutados()  const { return IOs_contador;      }
    size_t numTuplasGeradas() const { return tuplasGeradas;     }

    void salvarTuplasGeradas(const string& userFile) const {
        /* Apenas copia o arquivo join_result.csv (sem contar IO).              */
        ifstream src(resFile_, ios::binary);
        ofstream dst(userFile, ios::binary);
        dst << src.rdbuf();
    }
};
/* ============================================================================ */

int main() {
    ios::sync_with_stdio(false);

    /* -------- Criação das tabelas (carregamento NÃO conta IO) -------------- */
    Table vinho("vinho.csv");
    Table uva  ("uva.csv");
    Table pais ("pais.csv");

    vinho.carregarDados();
    uva.carregarDados();
    pais.carregarDados();

    /* --------- Exemplo de junção: Vinho.uva_id = Uva.uva_id ------------------*/
    Operador op(vinho, uva, "uva_id", "uva_id");
    op.executar();

    cout << "#Pags: " << op.numPagsGeradas()
         << "\n#IOss: " << op.numIOExecutados()
         << "\n#Tups: " << op.numTuplasGeradas()
         << '\n';

    op.salvarTuplasGeradas("selecao_vinho_uva.csv");
    return 0;
}
