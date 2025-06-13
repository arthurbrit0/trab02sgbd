#pragma once
#include <vector>
#include <string>
#include <utility>

constexpr std::size_t TUPLAS_POR_PAG  = 10;     // capacidade de uma página
constexpr std::size_t PAGS_BUFFER_MAX = 4;      // máximo de páginas simultâneas
constexpr char        CSV_SEP         = ',';    // separador CSV

/* ---------------- estrutura de uma tupla -----------------------------------*/
struct Tuple {
    std::vector<std::string> cols;
};

/* ---------------- estrutura de uma página ----------------------------------*/
class Page {
public:
    Page() { data_.reserve(TUPLAS_POR_PAG); }

    bool full()  const { return data_.size() == TUPLAS_POR_PAG; }
    bool empty() const { return data_.empty(); }

    void         emplace(Tuple&& t) { data_.emplace_back(std::move(t)); }
    void         emplace(const Tuple& t) { data_.push_back(t); }
    void         clear()            { data_.clear();                    }

    const std::vector<Tuple>& tuples() const { return data_; }
          std::vector<Tuple>& tuples()       { return data_; }

private:
    std::vector<Tuple> data_;
};
