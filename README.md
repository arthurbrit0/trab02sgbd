# Sort‑Merge‑Join com External Merge Sort

Trabalho prático de Sistema Gerenciador de Bancos de Dados — UFC  
Autores: Arthur Brito e João Vitor Mesquita Gouvea

## 1. Objetivo

Implementar **do zero** o operador de junção `R ⨝ S` usando o algoritmo
**Sort‑Merge Join (SMJ)**, atendendo às seguintes restrições:

| Requisito | Valor |
|-----------|-------|
| Páginas por bloco | **10 tuplas** |
| Páginas simultâneas em RAM | **≤ 4** |
| SGBD permitido | **Nenhum**. Utilizar apenas sistema de arquivos |
| Linguagem | C++17 |
| Métricas exibidas | Nº de I/Os, nº de páginas geradas, nº de tuplas resultantes |

## 2. Arquivos de entrada

As relações **Pais**, **Uva** e **Vinho** são fornecidas em CSV com cabeçalho.
Cada linha equivale a uma tupla e pode conter vírgulas dentro de campos
literais.

## 3. Arquitetura do projecto

| Camada | Arquivo(s) | Responsabilidade |
|--------|------------|------------------|
| **Modelos** | `Tuple`, `Page` | Estruturas básicas com tamanho controlado |
| **Persistência** | `Table` | Serialização e streaming de páginas; cálculo do número de colunas |
| **Medição** | `IoTracker` | Contagem transparente de páginas lidas / gravadas |
| **Algoritmos** | `ExternalSorter` | EMS completo (Passo 0 + k‑way merge) |
|                | `SortMergeJoin` | SMJ clássico com marcadores |
| **Aplicação** | `main.cpp` | Interface de linha de comando — exemplos de junções |

A **assertiva de segurança** `static_assert(PAGS_BUFFER_MAX >= 4)` garante que
nenhuma alteração acidental reduza o limite estabelecido.

## 4. Algoritmo de External Merge Sort

1. **Passo 0 (run generation)**  
   Carrega até 4 páginas → ordena em memória → grava *run* ordenado.  
   Repetido até esgotar arquivo.  
2. **Passos ≥ 1 (k‑way merge)**  
   *heap* mínimo com 3 fluxos de entrada + 1 página de saída.  
   Se ao final mais de um run persistir, executa‑se nova passada.

Complexidade:  
`O(#páginas × log₍k₎ #runs)` em I/O, onde `k=3`.

## 5. Sort‑Merge Join

Pré‑condição: relações ordenadas pelas chaves.  
`SMJ(A,B)` usa 1 pág. de `A`, 1 pág. de `B`, 1 pág. de saída.  
Quando `a.key == b.key`, grava todas as combinações  
(cardinalidade `|grupo_A| × |grupo_B|`).

Vantagens:

* só faz varredura sequencial (I/O contíguo);
* respeita limitação de memória imposta.

## 6. Compilação e execução

```bash
mkdir build && cd build
cmake ..                # requer CMake ≥3.15
cmake --build .         # gera binário smj
./smj                   # executa exemplo padrão

```

Para parâmetros opcionais, faz-se o seguinte:

```bash
./smj <arquivoA.csv> <arquivoB.csv> <colA> <colB> <saida.csv>
```

## 7. Exemplo de Saída

#IOs       : 734
#Pág.s grav: 298
#Tuplas res: 500

Os valores variam conforme a relação / predicado de junção.

## 8. Estruturas de Dados Fundamentais

### 8.1 Tuple
* **Responsabilidade**: representar uma única tupla (linha) de qualquer relação.
* **Implementação**: `struct Tuple { std::vector<std::string> cols; }`.
  * Não contém lógica própria; toda manipulação é feita pelas classes de nível superior.

### 8.2 Page
* **Capacidade**: `TUPLAS_POR_PAG` (10) tuplas.
* **Principais métodos**:
  * `full()` / `empty()` – verificam ocupação.
  * `emplace(Tuple&&)` e `emplace(const Tuple&)` – inserção por *move* ou cópia.
  * `clear()` – reinicializa conteúdo.
* **Motivação**: abstrair um bloco de disco; todas as operações de E/S contam páginas e não tuplas individuais.

### 8.3 Table
* **Construtor**: carrega **somente o cabeçalho** para descobrir as colunas.
* **PageCursor** (classe interna): faz *streaming* sequencial, lendo páginas sob demanda.
* **Métodos-chave**:
  * `header()` retorna nomes das colunas.
  * `colIndex(name)` obtém índice de uma coluna.

## 9. Medição de I/O – `IoTracker`
| Função | Descrição |
|--------|-----------|
| `reset()` | zera contadores antes de cada operação |
| `incRead()` / `incWrite()` | incrementam leituras/escritas de página |
| `operations()` | total de operações (I/O) |
| `pagesWritten()` | páginas efetivamente geradas em disco |

Implementado como variáveis `static` atômicas, permitindo contagem global e thread-safe (embora o projecto seja *single-thread*).

## 10. External Merge Sort em detalhes

### 10.1 Funções utilitárias
* `splitLine` – divide uma linha CSV respeitando vírgulas dentro de literais.
* `readPage` / `writePage` – conversão página ⇄ fluxo de arquivo, contabilizando E/S.

### 10.2 Passo 0 – **Geração de *runs***
1. O *cursor* (`Table::PageCursor`) lê até **4 páginas**.
2. Tuplas vão para `mem` (vector) e são ordenadas in-place (`std::sort`).
3. O *run* ordenado é descarregado em disco com cabeçalho.

### 10.3 Passos ≥ 1 – **k-way merge**
* Usa 3 páginas de entrada (A, B) + 1 página de saída.
* `mergeTwo` compara primeiras tuplas dos *runs* (já ordenadas) e realiza *merge* estável.
* Após consumir todos os runs da passada, `mergePass` gera nova deque para próxima iteração.

### 10.4 Garantia de Memória
`static_assert(PAGS_BUFFER_MAX >= 4)` impede compilações que reduzam o limite.

### 10.5 Complexidades
* **Tempo (I/O)** ‒ `O(#páginas × log₍3₎ #runs)`
* **Memória** ‒ ≤ 4 páginas (cerca de 40 tuplas).

## 11. Sort-Merge Join

| Fase | Método/função | Observações |
|------|---------------|-------------|
| Ordenação prévia | `externalSort` | chamada 2 × (A e B) |
| Leitura sequencial | `readPage` reutilizado | uma página por relação |
| Marcação de grupo | cópias de página + `seekg` | permite retroceder em B |
| Combinação de tuplas | *produto cartesiano* dentro do grupo | grava em página de saída |
| Escrita | `writeHeader` + flush de página | contabiliza I/O |

A função `sortMergeJoin` devolve `JoinStats` com métricas de desempenho.

## 12. Fluxo de Execução (`main.cpp`)
1. Garante existência da pasta `data`.
2. Constrói objetos `Table` para **vinho** e **uva**.
3. Executa `sortMergeJoin` nas chaves `uva_id`.
4. Grava resultado em `resultado_vinho_uva.csv` e imprime estatísticas.

### vinho ⨝ uva em uva_id = uva_id
./smj data/vinho.csv data/uva.csv uva_id uva_id resultado_vinho_uva.csv

### uva ⨝ pais em pais_origem_id = pais_id
./smj data/uva.csv data/pais.csv pais_origem_id pais_id resultado_uvapais.csv

### vinho ⨝ pais em pais_producao_id = pais_id
./smj data/vinho.csv data/pais.csv pais_producao_id pais_id resultado_vinhopais.csv

### 12.1 Modo CLI avançado
Se fornecidos 5 parâmetros, o `main` usa‐os como:
```
smj <relA.csv> <relB.csv> <colA> <colB> <saida.csv>
```
Caso contrário, roda o *demo* padrão.

## 13. Organização de Pastas
```
include/   # headers (.hpp) – API pública e modelos
src/       # implementação (.cpp)
data/      # CSVs de exemplo
build/     # artefatos gerados pelo CMake
```

## 14. Tratamento de Erros & Validações
* Verificação de abertura de arquivo (`throw runtime_error`).
* Sincronização de cabeçalhos – número de colunas preenchido com strings vazias.
* Caminhos relativos: o executável deve ser invocado a partir da raiz ou a pasta `data` copiada para o diretório atual.

## 15. Extensões Sugestivas
* **Buffers maiores**: alterar `PAGS_BUFFER_MAX` e recompilar.
* **Formato CSV diferente**: mudar `CSV_SEP`.
* **Chaves múltiplas**: adaptar função de comparação nas ordenações.
* **Paralelização**: possível em *passo 0* e nas fases de merge.

## 16. FAQ Rápido
| Pergunta | Resposta |
|----------|----------|
| "Por que não usar um SGBD?" | Restrições da disciplina: manipulação direta de arquivo. |
| "Posso usar 100 tuplas por página?" | Sim, mas atualize `TUPLAS_POR_PAG` e recompile. |
| "Como medir desempenho real?" | Conferir I/Os exibidos + usar `time` ou *profiler* do sistema. |

---
*Documentação expandida em 2025-06-13 para fins de apresentação.*

