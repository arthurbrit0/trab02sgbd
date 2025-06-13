#include "Table.hpp"
#include "SortMergeJoin.hpp"
#include <iostream>

int main(int argc, char* argv[]) {
    if (argc != 6) {
        std::cerr << "Uso: " 
                  << argv[0] 
                  << " <tabelaA.csv> <tabelaB.csv> <colA> <colB> <saida.csv>\n"
                  << "Exemplo:\n"
                  << "  " << argv[0]
                  << " data/vinho.csv data/pais.csv pais_producao_id pais_id  resultado_vinho_pais.csv\n";
        return 1;
    }

    try {
        // 1) monta as tabelas
        Table A(argv[1]);
        Table B(argv[2]);

        // 2) faz a junção usando colA = colB
        auto stats = sortMergeJoin(
            A, B,
            argv[3],  // nome da coluna na tabela A
            argv[4],  // nome da coluna na tabela B
            argv[5]   // arquivo de saída
        );

        // 3) imprime métricas
        std::cout 
            << "#I/Os       : " << stats.ioOps     << "\n"
            << "#Páginas out: " << stats.pagesOut  << "\n"
            << "#Tuplas out : " << stats.tuplesOut << "\n";

    } catch (const std::exception& e) {
        std::cerr << "Erro: " << e.what() << "\n";
        return 2;
    }

    return 0;
}
